FROM maven:3.5.2-jdk-8 AS builder

RUN mkdir -p /build
WORKDIR /build

COPY src ./src
COPY pom.xml ./
COPY stylecheck* ./
COPY codecov.yml ./

RUN mvn clean package
RUN mv target/mysql-replicator-*.jar target/mysql-replicator.jar

FROM openjdk:8u151-jdk AS release

RUN mkdir -p /replicator
RUN mkdir -p /etc/replicator
WORKDIR /replicator

COPY --from=builder /build/target/mysql-replicator.jar ./
COPY src/main/resources/log4j.properties ./

ENTRYPOINT ["java", "-jar", "mysql-replicator.jar", "--config-path", "/etc/replicator/replicator.yaml"]

FROM openjdk:8u151-jdk AS compose

COPY --from=release /replicator /replicator
RUN mkdir -p /etc/replicator

WORKDIR /replicator

COPY integration-tests/delayed-run.sh ./

RUN apt-get update && apt-get install -y netcat && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["./delayed-run.sh"]
