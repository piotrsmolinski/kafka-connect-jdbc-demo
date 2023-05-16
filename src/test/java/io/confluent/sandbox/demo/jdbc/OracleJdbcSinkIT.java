package io.confluent.sandbox.demo.jdbc;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.db.type.Request;
import org.assertj.db.type.Source;
import org.junit.function.ThrowingRunnable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.OutputFrame;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static io.confluent.sandbox.demo.jdbc.JacksonUtils.*;
import static io.confluent.sandbox.demo.jdbc.ResourceUtils.*;
import static io.confluent.sandbox.demo.jdbc.DatabaseUtils.*;
import static io.confluent.sandbox.demo.jdbc.Assertions.*;

public class OracleJdbcSinkIT {

  static TestEnvironment<?> environment;

  @BeforeAll
  static void beforeAll() {

    environment = TestEnvironment.newEnvironment()
            .withOracle();

    environment.start();

  }

  @AfterAll
  static void afterAll() {

    if (environment != null) environment.stop();

  }

  Deque<ThrowingRunnable> cleanup = new ArrayDeque<>();

  @BeforeEach
  void beforeEach() {
    this.cleanup = new LinkedList<>();
  }
  @AfterEach
  void afterEach() throws Throwable {
    while (!cleanup.isEmpty()) {
      cleanup.removeLast().run();
    }
  }

  @Test
  void testAvro() throws Exception {

    environment.createTopic("avro-0-users", 1, 4);
    cleanup.add(()->environment.deleteTopic("avro-0-users"));
    environment.createTopic("avro-0-dlq", 1, 4);
    cleanup.add(()->environment.deleteTopic("avro-0-dlq"));

    environment.deployConnector(
            "avro-0",
            parseYaml(readResource("connectors/avro-0.yaml"), asConfig()));

    cleanup.add(()->environment.deleteConnector("avro-0"));

    Producer<String, GenericRecord> producer = environment.createProducerClient(
            Map.ofEntries(
              Map.entry("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
              Map.entry("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer"),
              Map.entry("schema.registry.url", environment.getRegistryRestAddress())
            )
    );
    cleanup.add(()->producer.close());

    Schema schema = new Schema.Parser().parse(new String(yaml2json(readResource("schemas/user.avsc"))));
    GenericRecord record = new GenericRecordBuilder(schema)
            .set("USER_ID", "avro-0")
            .set("FIRST_NAME", "Max")
            .set("LAST_NAME", "Mustermann")
            .build();

    Future<RecordMetadata> future = producer.send(new ProducerRecord<>("avro-0-users", "avro-0", record));
    producer.flush();
    future.get();

    Source source = new Source(environment.getOracleConnectionUrl(), "tester", "7vLlfbecvQGy");

    Request request = waitUntilPresent(
            10_000L,
            source,
            "SELECT * FROM kafkaconnect.users WHERE user_id = ?",
            "avro-0"
    );

    assertThat(request).hasNumberOfRows(1);

  }

  @Test
  void testJsonWithSimpleConverter() throws Exception {

    environment.createTopic("json-0-users", 1, 4);
    cleanup.add(()->environment.deleteTopic("json-0-users"));
    environment.createTopic("json-0-dlq", 1, 4);
    cleanup.add(()->environment.deleteTopic("json-0-dlq"));

    ConcurrentLinkedDeque<OutputFrame> log = new ConcurrentLinkedDeque<>();
    environment.addConnectLogListener(log::add);

    environment.deployConnector(
            "json-0",
            parseYaml(readResource("connectors/json-0.yaml"), asConfig()));

    cleanup.add(()->environment.deleteConnector("json-0"));

    Producer<String, Object> producer = environment.createProducerClient(
            Map.ofEntries(
                    Map.entry("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
                    Map.entry("value.serializer", "io.confluent.kafka.serializers.KafkaJsonSerializer")
            )
    );
    cleanup.add(()->producer.close());

    Map<String, Object> record = Map.ofEntries(
            Map.entry("USER_ID", "json-0"),
            Map.entry("FIRST_NAME", "Adam"),
            Map.entry("LAST_NAME", "Mustermann")
    );

    Future<RecordMetadata> future = producer.send(new ProducerRecord<>("json-0-users", "json-0", record));
    producer.flush();
    future.get();

    Source source = new Source(environment.getOracleConnectionUrl(), "tester", "7vLlfbecvQGy");

    Request request = waitUntilPresent(
            10_000L,
            source,
            "SELECT * FROM kafkaconnect.users WHERE user_id = ?",
            "json-0"
    );

    assertThat(request).hasNumberOfRows(0);

    Consumer<String,Object> consumer = environment.createConsumerClient(
            Map.ofEntries(
                    Map.entry("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"),
                    Map.entry("value.deserializer", "io.confluent.kafka.serializers.KafkaJsonDeserializer"),
                    Map.entry("json.value.type", "java.lang.Object")
            )
    );
    cleanup.add(()->consumer.close());
    List<TopicPartition> dlqPartitions = consumer.partitionsFor("json-0-dlq").stream()
            .map(p->new TopicPartition(p.topic(), p.partition()))
            .collect(Collectors.toList());
    consumer.assign(dlqPartitions);
    consumer.seekToBeginning(dlqPartitions);
    ConsumerRecord<String,Object> dlqRecord = null;
    long dlqDeadline = System.currentTimeMillis() + 10_000L;
    pollLoop: for (;;) {
      long remaining = dlqDeadline - System.currentTimeMillis();
      if (remaining <= 0) break;
      for (ConsumerRecord<String,Object> r : consumer.poll(Duration.ofMillis(remaining))) {
        dlqRecord = r;
        break pollLoop;
      }
    }
    assertThat(dlqRecord).isNull();

    long logDeadline = System.currentTimeMillis() + 10_000L;
    for (;;) {
      long remaining = logDeadline - System.currentTimeMillis();
      if (remaining <= 0) {
        fail("Expected to see connector failure despite of DLQ");
      }
      // https://github.com/apache/kafka/blob/3.4.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerSinkTask.java#L612-L614
      OutputFrame frame = log.poll();
      if (frame.getUtf8String().contains("ERROR WorkerSinkTask{id=json-0-0} Task threw an uncaught and unrecoverable exception.")) break;
    }

  }


  @Test
  void testJsonWithCustomConverter() throws Exception {

    environment.createTopic("json-1-users", 1, 4);
    cleanup.add(()->environment.deleteTopic("json-1-users"));
    environment.createTopic("json-1-dlq", 1, 4);
    cleanup.add(()->environment.deleteTopic("json-1-dlq"));

    environment.deployConnector(
            "json-1",
            parseYaml(readResource("connectors/json-1.yaml"), asConfig()));

    cleanup.add(()->environment.deleteConnector("json-1"));

    Producer<String, Object> producer = environment.createProducerClient(
            Map.ofEntries(
              Map.entry("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
              Map.entry("value.serializer", "io.confluent.kafka.serializers.KafkaJsonSerializer")
            )
    );
    cleanup.add(()->producer.close());

    Map<String, Object> record = Map.ofEntries(
            Map.entry("USER_ID", "json-1"),
            Map.entry("FIRST_NAME", "Anne"),
            Map.entry("LAST_NAME", "Mustermann")
    );

    Future<RecordMetadata> future = producer.send(new ProducerRecord<>("json-1-users", "json-1", record));
    producer.flush();
    future.get();

    Source source = new Source(environment.getOracleConnectionUrl(), "tester", "7vLlfbecvQGy");

    Request request = waitUntilPresent(
            10_000L,
            source,
            "SELECT * FROM kafkaconnect.users WHERE user_id = ?",
            "json-1"
    );

    assertThat(request).hasNumberOfRows(1);

  }

  @Test
  void testJsonWithCast() throws Exception {

    environment.createTopic("json-2-users", 1, 4);
    cleanup.add(()->environment.deleteTopic("json-2-users"));
    environment.createTopic("json-2-dlq", 1, 4);
    cleanup.add(()->environment.deleteTopic("json-2-dlq"));

    environment.registerSchema(
            "json-2-users-value",
            "AVRO",
            new String(yaml2json(readResource("schemas/user.avsc"))));

    environment.deployConnector(
            "json-2",
            parseYaml(readResource("connectors/json-2.yaml"), asConfig()));

    cleanup.add(()->environment.deleteConnector("json-2"));

    Producer<String, Object> producer = environment.createProducerClient(
            Map.ofEntries(
              Map.entry("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
              Map.entry("value.serializer", "io.confluent.kafka.serializers.KafkaJsonSerializer")
            )
    );
    cleanup.add(()->producer.close());

    Map<String, Object> record = Map.ofEntries(
            Map.entry("USER_ID", "json-2"),
            Map.entry("FIRST_NAME", "Johann"),
            Map.entry("LAST_NAME", "Mustermann")
    );

    Future<RecordMetadata> future = producer.send(new ProducerRecord<>("json-2-users", "json-2", record));
    producer.flush();
    future.get();

    Source source = new Source(environment.getOracleConnectionUrl(), "tester", "7vLlfbecvQGy");

    Request request = waitUntilPresent(
            10_000L,
            source,
            "SELECT * FROM kafkaconnect.users WHERE user_id = ?",
            "json-2"
    );

    assertThat(request).hasNumberOfRows(1);

  }

  @Test
  void testJsonWithDetection() throws Exception {

    environment.createTopic("json-3-users", 1, 4);
    cleanup.add(()->environment.deleteTopic("json-3-users"));
    environment.createTopic("json-3-dlq", 1, 4);
    cleanup.add(()->environment.deleteTopic("json-3-dlq"));

    environment.deployConnector(
            "json-3",
            parseYaml(readResource("connectors/json-3.yaml"), asConfig()));

    cleanup.add(()->environment.deleteConnector("json-3"));

    Producer<String, Object> producer = environment.createProducerClient(
            Map.ofEntries(
              Map.entry("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
              Map.entry("value.serializer", "io.confluent.kafka.serializers.KafkaJsonSerializer")
            )
    );
    cleanup.add(()->producer.close());

    Map<String, Object> record = Map.ofEntries(
            Map.entry("USER_ID", "json-3"),
            Map.entry("FIRST_NAME", "Sophie"),
            Map.entry("LAST_NAME", "Mustermann")
    );

    Future<RecordMetadata> future = producer.send(new ProducerRecord<>("json-3-users", "json-3", record));
    producer.flush();
    future.get();

    Source source = new Source(environment.getOracleConnectionUrl(), "tester", "7vLlfbecvQGy");

    Request request = waitUntilPresent(
            10_000L,
            source,
            "SELECT * FROM kafkaconnect.users WHERE user_id = ?",
            "json-3"
    );

    assertThat(request).hasNumberOfRows(1);

  }

}
