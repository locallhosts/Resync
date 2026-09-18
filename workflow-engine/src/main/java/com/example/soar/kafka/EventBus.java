package com.example.soar.kafka;

import com.example.soar.events.Envelope;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Thin wrapper around the official Kafka client, matching the shape of
 * internal/kafkabus on the Go side: the rest of the engine depends on
 * {@link EventBus}, not on kafka-clients types directly.
 *
 * <p>Like the Go producer, every message is keyed by case ID
 * ({@link Envelope#caseId}) so a single case's events always land on
 * the same partition and are delivered in order — replay assumes that.
 */
public final class EventBus implements AutoCloseable {
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final String produceTopic;
    private final String consumeTopic;

    public EventBus(List<String> brokers, String produceTopic, String consumeTopic, String groupId) {
        this.produceTopic = produceTopic;
        this.consumeTopic = consumeTopic;

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", brokers));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        this.producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", brokers));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Auto-commit is fine here for the same reason it's fine on the
        // Go side: at-least-once delivery, made safe by idempotent
        // handling downstream (the event store's UNIQUE(case_id, seq)
        // constraint), not by delivery exactly-once guarantees.
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.consumer.subscribe(Collections.singletonList(consumeTopic));
    }

    public void publish(String key, Envelope envelope) {
        producer.send(new ProducerRecord<>(produceTopic, key, envelope.toJson()));
    }

    /**
     * Blocks up to {@code pollTimeout} for new messages on the subscribed
     * topic and invokes {@code handler} for each. Call this in a loop —
     * see WorkflowEngine's main loop.
     */
    public void poll(Duration pollTimeout, Consumer<Envelope> handler) {
        ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
        for (ConsumerRecord<String, String> record : records) {
            try {
                handler.accept(Envelope.fromJson(record.value()));
            } catch (RuntimeException ex) {
                System.err.println("EventBus: skipping unparsable message on " + consumeTopic
                        + " at offset " + record.offset() + ": " + ex.getMessage());
            }
        }
    }

    @Override
    public void close() {
        producer.close();
        consumer.close();
    }
}
