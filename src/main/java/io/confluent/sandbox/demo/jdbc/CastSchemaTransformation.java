package io.confluent.sandbox.demo.jdbc;

import io.confluent.connect.avro.AvroData;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public abstract class CastSchemaTransformation<R extends ConnectRecord<R>> implements Transformation<R> {

  protected Schema schema;

  @Override
  public R apply(R r) {
    return r.newRecord(
            r.topic(),
            r.kafkaPartition(),
            keySchema(r),
            key(r),
            valueSchema(r),
            value(r),
            r.timestamp(),
            r.headers()
    );
  }

  protected Schema keySchema(R r) {
    return r.keySchema();
  }

  protected Object key(R r) {
    return r.key();
  }

  protected Schema valueSchema(R r) {
    return r.valueSchema();
  }

  protected Object value(R r) {
    return r.value();
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

    String url = (String) map.get("schema.url");
    try {
      org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser()
              .parse(openUrl(url));
      this.schema = new AvroData(100).toConnectSchema(avroSchema);
    } catch (Exception e) {
      throw new ConfigException("Cannot construct schema from "+url, e);
    }

  }

  public static class Key<R extends ConnectRecord<R>> extends CastSchemaTransformation<R> {

    @Override
    protected Schema keySchema(R r) {
      return schema;
    }

    @Override
    protected Object key(R r) {
      return SchemaCaster.castTo(schema).apply(r.key());
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends CastSchemaTransformation<R> {

    @Override
    protected Schema valueSchema(R r) {
      return schema;
    }

    @Override
    protected Object value(R r) {
      return SchemaCaster.castTo(schema).apply(r.value());
    }

  }

  protected static InputStream openUrl(String url) {
    if (url.startsWith("http:") || url.startsWith("https:") || url.startsWith("file:")) {
      try {
        return new URL(url).openStream();
      } catch (IOException e) {
        throw new ConfigException("Cannot open URL "+url, e);
      }
    }
    if (url.startsWith("classpath:")) {
      return ClassLoader.getSystemResourceAsStream(url.substring("classpath:".length()));
    }
    if (url.startsWith("text:")) {
      return new ByteArrayInputStream(url.substring("text:".length()).getBytes(StandardCharsets.UTF_8));
    }
    throw new ConfigException("Unsupported URL: "+url);
  }

}
