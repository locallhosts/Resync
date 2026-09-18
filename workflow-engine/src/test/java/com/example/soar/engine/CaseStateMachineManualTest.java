package com.example.soar.engine;

import com.example.soar.events.*;
import java.util.*;

/**
 * A plain-main test harness, not JUnit — this project intentionally
 * has zero external dependencies beyond kafka-clients and postgresql
 * (see Json.java's javadoc for why), so pulling in a test framework
 * just for this one pure-logic class felt like the wrong trade.
 *
 * Run it with:
 *   mvn compile
 *   java -cp target/classes com.example.soar.engine.CaseStateMachineManualTest
 *
 * All 18 checks below were run and passed against this exact code
 * before it was committed — see the project README for how.
 */
public class CaseStateMachineManualTest {
    static int checks = 0;

    static void check(boolean cond, String msg) {
        checks++;
        if (!cond) throw new RuntimeException("FAIL: " + msg);
        System.out.println("ok: " + msg);
    }

    public static void main(String[] args) {
        UUID caseId = UUID.randomUUID();

        // 1. High severity alert with an IP -> should command BlockIP
        Envelope alert = Envelope.of(caseId, EventType.AlertReceived, Map.of(
                "source", "test", "severity", "high", "alert_raw", "suspicious traffic from 203.0.113.7"
        )).withSeq(1);
        CaseState s1 = CaseStateMachine.fold(caseId, List.of(alert));
        check(s1.status == CaseState.Status.ALERTED, "status is ALERTED after alert");
        Decision d1 = CaseStateMachine.decide(s1);
        check(d1.kind == Decision.Kind.COMMAND, "decides to command an action");
        check("BlockIP".equals(d1.action), "commands BlockIP");
        check("203.0.113.7".equals(d1.params.get("ip")), "extracts the right IP");

        // 2. Low severity -> no auto action
        Envelope lowAlert = Envelope.of(caseId, EventType.AlertReceived, Map.of(
                "source", "test", "severity", "low", "alert_raw", "minor anomaly from 10.0.0.5"
        )).withSeq(1);
        CaseState s2 = CaseStateMachine.fold(caseId, List.of(lowAlert));
        Decision d2 = CaseStateMachine.decide(s2);
        check(d2.kind == Decision.Kind.NONE, "low severity produces no decision");

        // 3. Commanded -> succeeded -> should close as resolved
        Envelope commanded = Envelope.of(caseId, EventType.ActionCommanded, Map.of(
                "command_id", UUID.randomUUID().toString(), "action", "BlockIP",
                "params", Map.of("ip", "203.0.113.7", "reason", "x")
        )).withSeq(2);
        Envelope succeeded = Envelope.of(caseId, EventType.ActionSucceeded, Map.of(
                "command_id", UUID.randomUUID().toString(), "action", "BlockIP", "result", Map.of()
        )).withSeq(3);
        CaseState s3 = CaseStateMachine.fold(caseId, List.of(alert, commanded, succeeded));
        check(s3.status == CaseState.Status.SUCCEEDED, "status is SUCCEEDED");
        Decision d3 = CaseStateMachine.decide(s3);
        check(d3.kind == Decision.Kind.CLOSE, "decides to close");
        check("resolved".equals(d3.closeReason), "closes with reason=resolved");

        // 4. Failed once -> should retry with SAME action + params (this
        //    is the property the recovery/replay demo depends on)
        Envelope failed1 = Envelope.of(caseId, EventType.ActionFailed, Map.of(
                "command_id", UUID.randomUUID().toString(), "action", "BlockIP",
                "error", "timeout", "retryable", true
        )).withSeq(3);
        CaseState s4 = CaseStateMachine.fold(caseId, List.of(alert, commanded, failed1));
        check(s4.status == CaseState.Status.FAILED, "status is FAILED");
        check(s4.failureCount == 1, "failureCount is 1");
        Decision d4 = CaseStateMachine.decide(s4);
        check(d4.kind == Decision.Kind.COMMAND, "retries after first failure");
        check("BlockIP".equals(d4.action), "retries the SAME action");
        check("203.0.113.7".equals(d4.params.get("ip")), "retries with the SAME params");

        // 5. Failed MAX_RETRIES+1 times -> should give up and close
        List<Envelope> history = new ArrayList<>(List.of(alert, commanded));
        for (int i = 0; i < 3; i++) {
            history.add(Envelope.of(caseId, EventType.ActionFailed, Map.of(
                    "command_id", UUID.randomUUID().toString(), "action", "BlockIP",
                    "error", "timeout", "retryable", true
            )).withSeq(3 + i));
        }
        CaseState s5 = CaseStateMachine.fold(caseId, history);
        check(s5.failureCount == 3, "failureCount is 3 after 3 failures");
        Decision d5 = CaseStateMachine.decide(s5);
        check(d5.kind == Decision.Kind.CLOSE, "gives up after max retries");
        check("action_failed_max_retries".equals(d5.closeReason), "closes with reason=action_failed_max_retries");

        // 6. Recovery scenario: replaying from scratch after a "crash"
        //    right after the command was issued (no outcome yet) should
        //    NOT re-command — engine waits for the sandbox worker's
        //    result, matching RecoveryRunner's behavior of re-publishing
        //    the command rather than re-deciding it from alert state.
        CaseState s6 = CaseStateMachine.fold(caseId, List.of(alert, commanded));
        check(s6.status == CaseState.Status.COMMANDED, "status is COMMANDED with outcome pending");
        Decision d6 = CaseStateMachine.decide(s6);
        check(d6.kind == Decision.Kind.NONE, "engine does not re-decide while a command is outstanding");

        System.out.println();
        System.out.println("ALL " + checks + " CHECKS PASSED");
    }
}
