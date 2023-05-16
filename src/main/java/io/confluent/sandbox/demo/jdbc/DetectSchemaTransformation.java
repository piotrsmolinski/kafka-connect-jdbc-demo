package io.confluent.sandbox.demo.jdbc;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class DetectSchemaTransformation<R extends ConnectRecord<R>> implements Transformation<R> {

  @Override
  public R apply(R r) {
    SchemaAndValue key = detectKey(r);
    SchemaAndValue value = detectValue(r);
    return r.newRecord(
            r.topic(),
            r.kafkaPartition(),
            key.schema(),
            key.value(),
            value.schema(),
            value.value(),
            r.timestamp(),
            r.headers()
    );
  }

  protected SchemaAndValue detectKey(R r) {
    return new SchemaAndValue(r.keySchema(), r.key());
  }
  protected SchemaAndValue detectValue(R r) {
    return new SchemaAndValue(r.valueSchema(), r.value());
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {
  }

  public static class Key<R extends ConnectRecord<R>> extends DetectSchemaTransformation<R> {

    @Override
    protected SchemaAndValue detectKey(R r) {
      return detectSchema(r.key());
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends DetectSchemaTransformation<R> {

    @Override
    protected SchemaAndValue detectValue(R r) {
      return detectSchema(r.value());
    }

  }


  protected SchemaAndValue detectSchema(Object value) {
    Schema schema = new SchemaDetector().detectSchema(null, value);
    return new SchemaAndValue(schema, SchemaCaster.castTo(schema).apply(value));
  }

}
