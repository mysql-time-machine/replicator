FROM maven:3.5.2-jdk-8

RUN mkdir -p /build
WORKDIR /build

COPY src ./src
COPY pom.xml ./
COPY stylecheck* ./
COPY codecov.yml ./

RUN mvn clean package
RUN mv target/mysql-replicator-*.jar target/mysql-replicator.jar

FROM openjdk:8u151-jdk

RUN mkdir -p /replicator
RUN mkdir -p /etc/replicator
WORKDIR /replicator

COPY --from=0 /build/target/mysql-replicator.jar ./
COPY src/main/resources/log4j.properties ./

ENTRYPOINT ["java", "-jar", "mysql-replicator.jar", "--config-path", "/etc/replicator/replicator.yaml"]