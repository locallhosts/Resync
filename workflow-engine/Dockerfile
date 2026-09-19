# Multi-stage build: Maven only exists in the build stage; the final
# image is a JRE (not full JDK) plus the shaded jar.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/target/workflow-engine.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
