package io.confluent.sandbox.demo.jdbc;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class SchemaDetector {

  private List<BiFunction<Schema, Object, Schema>> detectors;

  public SchemaDetector() {
    this.detectors = Arrays.asList(
      this::detectSchemaNull,
      this::detectSchemaBytes,
      this::detectSchemaString,
      this::detectSchemaByte,
      this::detectSchemaShort,
      this::detectSchemaInteger,
      this::detectSchemaLong,
      this::detectSchemaFloat,
      this::detectSchemaDouble,
      this::detectSchemaMap,
      this::detectSchemaCollection
    );
  }

  public Schema detectSchema(Schema lastSchema, Object value) {

    if (lastSchema != null && lastSchema.type() == Schema.Type.STRUCT && lastSchema.fields().isEmpty()) {
      lastSchema = null;
    }

    for (BiFunction<Schema, Object, Schema> detector : detectors) {
      Schema schema = detector.apply(lastSchema, value);
      if (schema != null) return schema;
    }

    throw new ConnectException("Cannot convert: "+value.getClass());

  }

  public Schema detectSchemaNull(Schema lastSchema, Object value) {
    if (value != null) {
      return null;
    }
    if (lastSchema == null) {
      return SchemaBuilder.struct().optional().build();
    }
    return lastSchema;
  }

  public Schema detectSchemaBytes(Schema lastSchema, Object value) {
    if (!(value instanceof byte[])) {
      return null;
    }
    if (lastSchema != null && lastSchema.type() == Schema.Type.STRUCT && lastSchema.fields().isEmpty()) {
      lastSchema = null;
    }
    if (lastSchema == null) {
      return Schema.OPTIONAL_BYTES_SCHEMA;
    }
    if (lastSchema.type() != Schema.Type.BYTES) {
      throw new ConnectException("Cannot convert: "+value.getClass());
    }
    return lastSchema;
  }

  public Schema detectSchemaString(Schema lastSchema, Object value) {
    if (!(value instanceof String)) {
      return null;
    }
    if (lastSchema == null || lastSchema.type() == Schema.Type.STRUCT && lastSchema.fields().isEmpty()) {
      return Schema.OPTIONAL_STRING_SCHEMA;
    }
    if (lastSchema.type() != Schema.Type.STRING) {
      throw new ConnectException("Cannot convert: "+value.getClass());
    }
    return lastSchema;
  }

  public Schema detectSchemaByte(Schema lastSchema, Object value) {
    if (!(value instanceof Byte)) {
      return null;
    }
    if (lastSchema == null || lastSchema.type() == Schema.Type.STRUCT && lastSchema.fields().isEmpty()) {
      return Schema.OPTIONAL_INT8_SCHEMA;
    }
    switch (lastSchema.type()) {
      case INT8:
        return Schema.OPTIONAL_INT8_SCHEMA;
      case INT16:
        return Schema.OPTIONAL_INT16_SCHEMA;
      case INT32:
        return Schema.OPTIONAL_INT32_SCHEMA;
      case INT64:
        return Schema.OPTIONAL_INT64_SCHEMA;
      case FLOAT32:
        return Schema.OPTIONAL_FLOAT32_SCHEMA;
      case FLOAT64:
        return Schema.OPTIONAL_FLOAT64_SCHEMA;
      default:
        throw new ConnectException("Cannot convert: "+value.getClass());
    }
  }

  public Schema detectSchemaShort(Schema lastSchema, Object value) {
    if (!(value instanceof Short)) {
      return null;
    }
    if (lastSchema == null || lastSchema.type() == Schema.Type.STRUCT && lastSchema.fields().isEmpty()) {
      return Schema.OPTIONAL_INT16_SCHEMA;
    }
    switch (lastSchema.type()) {
      case INT8:
      case INT16:
        return Schema.OPTIONAL_INT16_SCHEMA;
      case INT32:
        return Schema.OPTIONAL_INT32_SCHEMA;
      case INT64:
        return Schema.OPTIONAL_INT64_SCHEMA;
      case FLOAT32:
        return Schema.OPTIONAL_FLOAT32_SCHEMA;
      case FLOAT64:
        return Schema.OPTIONAL_FLOAT64_SCHEMA;
      default:
        throw new ConnectException("Cannot convert: "+value.getClass());
    }
  }

  public Schema detectSchemaInteger(Schema lastSchema, Object value) {
    if (!(value instanceof Integer)) {
      return null;
    }
    if (lastSchema == null || lastSchema.type() == Schema.Type.STRUCT && lastSchema.fields().isEmpty()) {
      return Schema.OPTIONAL_INT32_SCHEMA;
    }
    switch (lastSchema.type()) {
      case INT8:
      case INT16:
      case INT32:
        return Schema.OPTIONAL_INT32_SCHEMA;
      case INT64:
        return Schema.OPTIONAL_INT64_SCHEMA;
      case FLOAT32:
        return Schema.OPTIONAL_FLOAT32_SCHEMA;
      case FLOAT64:
        return Schema.OPTIONAL_FLOAT64_SCHEMA;
      default:
        throw new ConnectException("Cannot convert: "+value.getClass());
    }
  }

  public Schema detectSchemaLong(Schema lastSchema, Object value) {
    if (!(value instanceof Long)) {
      return null;
    }
    if (lastSchema == null || lastSchema.type() == Schema.Type.STRUCT && lastSchema.fields().isEmpty()) {
      return Schema.OPTIONAL_INT64_SCHEMA;
    }
    switch (lastSchema.type()) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
        return Schema.OPTIONAL_INT64_SCHEMA;
      case FLOAT32:
        return Schema.OPTIONAL_FLOAT32_SCHEMA;
      case FLOAT64:
        return Schema.OPTIONAL_FLOAT64_SCHEMA;
      default:
        throw new ConnectException("Cannot convert: "+value.getClass());
    }
  }

  public Schema detectSchemaFloat(Schema lastSchema, Object value) {
    if (!(value instanceof Float)) {
      return null;
    }
    if (lastSchema == null || lastSchema.type() == Schema.Type.STRUCT && lastSchema.fields().isEmpty()) {
      return Schema.OPTIONAL_FLOAT32_SCHEMA;
    }
    switch (lastSchema.type()) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
        return Schema.OPTIONAL_FLOAT32_SCHEMA;
      case FLOAT64:
        return Schema.OPTIONAL_FLOAT64_SCHEMA;
      default:
        throw new ConnectException("Cannot convert: "+value.getClass());
    }
  }

  public Schema detectSchemaDouble(Schema lastSchema, Object value) {
    if (!(value instanceof Short)) {
      return null;
    }
    if (lastSchema == null || lastSchema.type() == Schema.Type.STRUCT && lastSchema.fields().isEmpty()) {
      return Schema.OPTIONAL_FLOAT64_SCHEMA;
    }
    switch (lastSchema.type()) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
        return Schema.OPTIONAL_FLOAT64_SCHEMA;
      default:
        throw new ConnectException("Cannot convert: "+value.getClass());
    }
  }

  public Schema detectSchemaMap(Schema lastSchema, Object value) {
    if (!(value instanceof Map)) {
      return null;
    }
    if (lastSchema == null) {
      lastSchema = SchemaBuilder.struct().optional().build();
    } else if (lastSchema.type() != Schema.Type.STRUCT) {
      throw new DataException("Cannot convert: "+value.getClass());
    }
    SchemaBuilder builder = SchemaBuilder.struct().optional();
    for (Field field : lastSchema.fields()) {
      builder.field(field.name(), detectSchema(field.schema(), ((Map<?,?>)value).get(field.name())));
    }
    for (Map.Entry<?,?> e : ((Map<?,?>)value).entrySet()) {
      if (!(e.getKey() instanceof String)) {
        throw new DataException("Cannot map key: "+e.getKey().getClass());
      }
      Field lastField = lastSchema.field((String)e.getKey());
      if (lastField != null) {
        continue;
      }
      builder.field((String)e.getKey(), detectSchema(null, e.getValue()));
    }
    return builder.build();
  }

  public Schema detectSchemaCollection(Schema lastSchema, Object value) {
    if (!(value instanceof Collection)) {
      return null;
    }
    Schema elementSchema;
    if (lastSchema == null || lastSchema.type() == Schema.Type.STRUCT && lastSchema.fields().isEmpty()) {
      elementSchema = SchemaBuilder.struct().optional().build();
    } else if (lastSchema.type() == Schema.Type.ARRAY) {
      elementSchema = lastSchema.valueSchema();
    } else {
      throw new DataException("Cannot convert: "+value.getClass());
    }
    for (Object element : (Collection<?>)value) {
      elementSchema = detectSchema(elementSchema, element);
    }
    return SchemaBuilder.array(elementSchema).optional().build();
  }

}
