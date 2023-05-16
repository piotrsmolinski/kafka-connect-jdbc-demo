package io.confluent.sandbox.demo.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class TestEnvironment<SELF extends TestEnvironment<SELF>> {

  protected static String confluentVersion =
          System.getProperty("confluent.version", "7.4.0");

  protected Network network;

  protected KafkaContainer kafka;

  protected GenericContainer<?> registry;

  protected GenericContainer<?> connect;

  protected boolean startOracle = false;
  protected GenericContainer<?> oracle;

  protected boolean startPostgres = false;
  protected GenericContainer<?> postgres;

  public static TestEnvironment newEnvironment() {

    return new TestEnvironment();

  }

  public TestEnvironment withOracle() {
    this.startOracle = true;
    return self();
  }

  public TestEnvironment withPostgres() {
    this.startPostgres = true;
    return self();
  }

  public GenericContainer<?> getOracle() {
    return oracle;
  }

  public String getOracleConnectionUrl() {
    return "jdbc:oracle:thin:@" + oracle.getHost() + ":" + oracle.getMappedPort(1521) + "/xepdb1";
  }

  public String getPostgresConnectionUrl() {
    return "jdbc:postgresql://"+postgres.getHost()+":"+ postgres.getMappedPort(5432)+"/connect";
  }

  public GenericContainer<?> getPostgres() {
    return postgres;
  }

  public final void start() {

    network = Network.newNetwork();

    createContainers();

    startContainers();

  }

  public final void stop() {

    stopContainers();

    if (network != null) network.close();

  }

  protected void createContainers() {

    kafka = createKafka();
    registry = createRegistry();
    connect = createConnect();
    oracle = createOracle();
    postgres = createPostgres();

  }

  protected void startContainers() {

    kafka.start();
    registry.start();
    connect.start();
    if (startOracle) oracle.start();
    if (startPostgres) postgres.start();

  }

  protected void stopContainers() {

    if (startPostgres) postgres.stop();
    if (startOracle) oracle.stop();
    connect.stop();
    registry.stop();
    kafka.stop();

  }

  protected KafkaContainer createKafka() {
    KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag(confluentVersion))
            .withNetwork(network)
            .withNetworkAliases("kafka")
//            .withKraft()
    ;
    return kafka;
  }

  protected GenericContainer<?> createRegistry() {
    GenericContainer<?> registry = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry").withTag(confluentVersion))
            .withNetwork(network)
            .withNetworkAliases("registry")
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
            .withEnv("SCHEMA_REGISTRY_OPTS", "")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL", "PLAINTEXT")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_REPLICATION_FACTOR", "1")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC", "_schemas")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID", "registry")
            .withEnv("SCHEMA_REGISTRY_LOG4J_ROOT_LOGLEVEL", "INFO")
            .withEnv("SCHEMA_REGISTRY_LOG4J_LOGGERS", "io.confluent.license.LicenseManager=WARN")
            .waitingFor(Wait.forLogMessage(".*Server started, listening for requests.*", 1))
    ;
    return registry;
  }

  protected GenericContainer<?> createConnect() {
    String connectImage = System.getProperty("connect.image",
            "kafka-connect-jdbc-demo:latest");

    GenericContainer<?> connect = new GenericContainer<>(DockerImageName.parse(connectImage))
            .withNetwork(network)
            .withNetworkAliases("connect")
            .withExposedPorts(8083)
            .withEnv("CONNECT_BOOTSTRAP_SERVERS", "kafka:9092")
            .withEnv("CONNECT_SECURITY_PROTOCOL", "PLAINTEXT")

            .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "localhost")

            .withEnv("CONNECT_GROUP_ID", "__connect")
            .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "__connect-config")
            .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "__connect-offset")
            .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "__connect-status")
            .withEnv("CONNECT_REPLICATION_FACTOR", String.valueOf(1))
            .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", String.valueOf(1))
            .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", String.valueOf(1))
            .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", String.valueOf(1))
            .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
            .withEnv("CONNECT_HEADER_CONVERTER", "org.apache.kafka.connect.converters.ByteArrayConverter")
            .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
            .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/usr/share/confluent-hub-components,/usr/share/filestream-connectors")
            .withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO")
            .withEnv("CONNECT_LOG4J_LOGGERS", "org.reflections=ERROR")
            .waitingFor(
                    Wait.forLogMessage(".*Kafka Connect started.*", 1)
                            .withStartupTimeout(Duration.ofMinutes(5))
            )
            .withLogConsumer(this::forwardLog)
            .withLogConsumer(f->connectLogListeners.get().forEach(c->c.accept(f)));
            ;
    return connect;
  }

  private AtomicReference<List<java.util.function.Consumer<OutputFrame>>> connectLogListeners =
          new AtomicReference<>(Collections.emptyList());

  public void addConnectLogListener(java.util.function.Consumer<OutputFrame> consumer) {
    List<java.util.function.Consumer<OutputFrame>> oldListeners = connectLogListeners.get();
    List<java.util.function.Consumer<OutputFrame>> newListeners = new ArrayList<>(oldListeners.size()+1);
    newListeners.addAll(oldListeners);
    newListeners.add(consumer);
    connectLogListeners.set(newListeners);
  }
  public void clearConnectLogListeners() {
    connectLogListeners.set(Collections.emptyList());
  }

  protected GenericContainer<?> createOracle() {

    GenericContainer<?> oracle = new GenericContainer<>(DockerImageName.parse("container-registry.oracle.com/database/express:21.3.0-xe"))
            .withNetwork(network)
            .withNetworkAliases("oracle")
            .withExposedPorts(1521, 5500)
            // XE comes with preinitialized database, therefore it is faster to start
            // the drawback is that the setup scripts are not executed
//            .withFileSystemBind("src/test/docker/oracle/setup", "/opt/oracle/scripts/setup")
            .withFileSystemBind("src/test/docker/oracle/startup", "/opt/oracle/scripts/startup")
            //
            .withFileSystemBind("src/test/docker/oracle/sqlnet.ora", "/opt/oracle/oradata/dbconfig/XE/sqlnet.ora")
            // DATABASE IS READY TO USE is printed out after setup scripts before startup scripts
            .waitingFor(new LogMessageWaitStrategy()
                    // "DATABASE IS READY TO USE" is printed out before startup scripts are run
//                    .withRegEx(".*DATABASE IS READY TO USE.*")
                    // therefore we need the last script to
            .withRegEx(".*USER SCRIPTS COMPLETED.*")
            .withTimes(1)
            .withStartupTimeout(Duration.ofMinutes(2)))
//            .withLogConsumer(this::forwardLog)
            ;

    return oracle;

  }

  protected GenericContainer<?> createPostgres() {

    GenericContainer<?> postgres = new GenericContainer<>(DockerImageName.parse("postgres:11.16"))
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withExposedPorts(5432)
            .withEnv("POSTGRES_DB", "postgres")
            .withEnv("POSTGRES_USER", "postgres")
            .withEnv("POSTGRES_PASSWORD", "dR5q0WwL")
            .withFileSystemBind("src/test/docker/postgres/setup", "/docker-entrypoint-initdb.d")
            .waitingFor(new LogMessageWaitStrategy()
                    .withRegEx(".*database system is ready to accept connections.*")
                    // postgres restarts the db after running setup scripts
                    .withTimes(2)
                    .withStartupTimeout(Duration.ofMinutes(2)))
            .withLogConsumer(this::forwardLog)
            ;

    return postgres;

  }

  public void createTopic(String name, int replicationFactor, int numPartitions) {
    try (Admin admin = createAdminClient()) {
      admin.createTopics(Arrays.asList(
              new NewTopic(
                      name,
                      numPartitions,
                      (short)replicationFactor
              )
      )).all().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void deleteTopic(String name) {
    try (Admin admin = createAdminClient()) {
      admin.deleteTopics(Arrays.asList(
              name
      )).all().get();
    } catch (InterruptedException|ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public Admin createAdminClient() {
    return createAdminClient(Collections.emptyMap());
  }

  public Admin createAdminClient(Map<String,?> overrides) {
    Map<String,Object> config = new LinkedHashMap<>();
    config.put("bootstrap.servers", kafka.getHost()+":"+ kafka.getMappedPort(9093));
    config.putAll(overrides);
    return Admin.create(config);
  }

  public <K,V> Consumer<K,V> createConsumerClient() {
    return createConsumerClient(Collections.emptyMap());
  }

  public <K,V> Consumer<K,V> createConsumerClient(Map<String,?> overrides) {
    Map<String,Object> config = new LinkedHashMap<>();
    config.put("bootstrap.servers", kafka.getHost()+":"+ kafka.getMappedPort(9093));
    config.putAll(overrides);
    return new KafkaConsumer<>(config);
  }

  public <K,V> Producer<K,V> createProducerClient() {
    return createProducerClient(Collections.emptyMap());
  }

  public <K,V> Producer<K,V> createProducerClient(Map<String,?> overrides) {
    Map<String,Object> config = new LinkedHashMap<>();
    config.put("bootstrap.servers", kafka.getHost()+":"+ kafka.getMappedPort(9093));
    config.putAll(overrides);
    return new KafkaProducer<>(config);
  }

  public void deployConnector(String connectorName, Map<String,?> config) throws Exception {

    HttpClient client = HttpClient.newHttpClient();

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(getConnectRestAddress()+"/connectors/"+connectorName+"/config"))
            .header("Content-Type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(JacksonUtils.renderJson(config)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() == 201) {
      // ok
    } else {
      throw new Exception("Expected response code 201, got "+response.statusCode()+"\nResponse:"+response.body());
    }

  }

  public void deleteConnector(String connectorName) throws Exception {

    HttpClient client = HttpClient.newHttpClient();

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(getConnectRestAddress()+"/connectors/"+connectorName))
            .DELETE()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() == 204) {
      // ok
    } else if (response.statusCode() == 404) {
      // ok
    } else {
      throw new Exception("Expected response code 201, got "+response.statusCode()+"\nResponse:"+response.body());
    }

  }

  public int registerSchema(String subject, String schemaType, String schema) throws Exception {

    HttpClient client = HttpClient.newHttpClient();

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(getRegistryRestAddress()+"/subjects/"+subject+"/versions"))
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .POST(
                    HttpRequest.BodyPublishers.ofString(JacksonUtils.renderJson(
                            new LinkedHashMap() {{
                              // specifying version and id requires the subject to be in the IMPORT mode
//                              if (version != null) put("version", version);
//                              if (id != null) put("id", id);
                              put("schemaType", schemaType);
                              put("schema", schema);
                            }}
                    ))
            )
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() == 200) {
      Map<String, Object> responseObj = JacksonUtils.parseJson(response.body(), new TypeReference<>() {});
      return (Integer)responseObj.get("id");
    } else {
      throw new Exception("Expected response code 200, got "+response.statusCode()+"\nResponse:"+response.body());
    }

  }


  public String getConnectRestAddress() {
    return "http://"+connect.getHost()+":"+connect.getMappedPort(8083);
  }
  public String getRegistryRestAddress() {
    return "http://"+registry.getHost()+":"+registry.getMappedPort(8081);
  }


  protected void forwardLog(OutputFrame frame) {
    switch (frame.getType()) {
      case STDOUT:
        System.out.println(frame.getUtf8String().stripTrailing());
        break;
      case STDERR:
        System.err.println(frame.getUtf8String().stripTrailing());
        break;
    }
  }

  protected SELF self() {
    return (SELF) this;
  }

}
