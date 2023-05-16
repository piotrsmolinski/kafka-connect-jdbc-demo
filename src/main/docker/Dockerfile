FROM maven:3.9.1 AS build

# Inhibit warning cloning from a tag
# Here the library is built from the sources, but it could be equally pulled from Nexus
RUN git clone --depth 1 https://github.com/purbon/kafka-connect-transformations.git
WORKDIR /kafka-connect-transformations/smt
RUN mvn package

FROM confluentinc/cp-kafka-connect-base:${confluent.version}

USER root

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.1

COPY maven/lib/jmx_prometheus_javaagent-0.18.0.jar /opt/prometheus/jmx_prometheus_javaagent.jar
COPY maven/lib/jolokia-jvm-1.7.2.jar /opt/jolokia/jolokia-jvm.jar
ADD kafka-client.yaml /opt/prometheus

COPY maven/lib/jul-to-slf4j-1.7.30.jar /usr/share/java/kafka/
COPY logging.properties /usr/lib/jvm/zulu11-ca/conf/logging.properties

COPY --chown=appuser:appuser log4j.properties.template /etc/confluent/docker/log4j.properties.template

COPY --chown=appuser:appuser run.sh /etc/confluent/docker/run
RUN chmod 755 /etc/confluent/docker/run

# Pere's custom JSON converter
COPY --chown=appuser:appuser --from=build /kafka-connect-transformations/smt/target/smt-1.0.jar /usr/share/java/kafka/

ADD --chown=appuser:appuser maven/connect/${project.artifactId}-${project.version}-distribution.tar.gz /usr/share/java/

USER appuser
