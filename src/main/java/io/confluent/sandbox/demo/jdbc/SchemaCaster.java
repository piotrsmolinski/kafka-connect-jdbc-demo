package io.confluent.sandbox.demo.jdbc;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SchemaCaster {

  protected static Map<Schema.Type, BiFunction<Object, Schema, ? extends Object>> conversions =
          Map.ofEntries(
                  Map.entry(Schema.Type.INT8, SchemaCaster::castInt8),
                  Map.entry(Schema.Type.INT16, SchemaCaster::castInt16),
                  Map.entry(Schema.Type.INT32, SchemaCaster::castInt32),
                  Map.entry(Schema.Type.INT64, SchemaCaster::castInt64),
                  Map.entry(Schema.Type.BOOLEAN, SchemaCaster::castBoolean),
                  Map.entry(Schema.Type.FLOAT32, SchemaCaster::castFloat32),
                  Map.entry(Schema.Type.FLOAT64, SchemaCaster::castFloat64),
                  Map.entry(Schema.Type.STRING, SchemaCaster::castString),
                  Map.entry(Schema.Type.BYTES, SchemaCaster::castBytes),
                  Map.entry(Schema.Type.ARRAY, SchemaCaster::castArray),
                  Map.entry(Schema.Type.MAP, SchemaCaster::castMap),
                  Map.entry(Schema.Type.STRUCT, SchemaCaster::castStruct)
          );

  protected static Byte castInt8(Object content, Schema schema) {
//    return Values.convertToByte(null, content);
    if (content instanceof Byte) {
      return (Byte) content;
    }
    if (content instanceof Short) {
      short x = (Short) content;
      if (x < (short)Byte.MIN_VALUE || x > (short)Byte.MAX_VALUE) {
        // warn
      }
      return ((Short) content).byteValue();
    }
    if (content instanceof Integer) {
      int x = (Integer) content;
      if (x < (int)Byte.MIN_VALUE || x > (int)Byte.MAX_VALUE) {
        // warn
      }
      return ((Integer) content).byteValue();
    }
    if (content instanceof Long) {
      long x = (Long) content;
      if (x < (long)Byte.MIN_VALUE || x > (long)Byte.MAX_VALUE) {
        // warn
      }
      return ((Long) content).byteValue();
    }
    if (content instanceof Number) {
      // warn
      return ((Number) content).byteValue();
    }
    if (content != null) {
      throw new DataException("Cannot cast "+content.getClass()+" to INT8");
    }
    if (schema.defaultValue() != null) {
      return (Byte)schema.defaultValue();
    }
    if (schema.isOptional()) {
      return null;
    }
    throw new DataException("Missing mandatory value");
  }

  protected static Short castInt16(Object content, Schema schema) {
    if (content instanceof Byte) {
      return ((Byte) content).shortValue();
    }
    if (content instanceof Short) {
      return (Short) content;
    }
    if (content instanceof Integer) {
      int x = (Integer) content;
      if (x < (int)Short.MIN_VALUE || x > (int)Short.MAX_VALUE) {
        // warn
      }
      return ((Integer) content).shortValue();
    }
    if (content instanceof Long) {
      long x = (Long) content;
      if (x < (long)Short.MIN_VALUE || x > (long)Short.MAX_VALUE) {
        // warn
      }
      return ((Long) content).shortValue();
    }
    if (content instanceof Number) {
      // warn
      return ((Number) content).shortValue();
    }
    if (content != null) {
      throw new DataException("Cannot cast "+content.getClass()+" to INT16");
    }
    if (schema.defaultValue() != null) {
      return (Short) schema.defaultValue();
    }
    if (schema.isOptional()) {
      return null;
    }
    throw new DataException("Missing mandatory value");
  }

  protected static Integer castInt32(Object content, Schema schema) {
    if (content instanceof Byte) {
      return ((Byte) content).intValue();
    }
    if (content instanceof Short) {
      return ((Short) content).intValue();
    }
    if (content instanceof Integer) {
      return (Integer) content;
    }
    if (content instanceof Long) {
      long x = (Long) content;
      if (x < (long)Integer.MIN_VALUE || x > (long)Integer.MAX_VALUE) {
        // warn
      }
      return ((Long) content).intValue();
    }
    if (content instanceof Number) {
      // warn
      return ((Number) content).intValue();
    }
    if (content != null) {
      throw new DataException("Cannot cast "+content.getClass()+" to INT32");
    }
    if (schema.defaultValue() != null) {
      return (Integer) schema.defaultValue();
    }
    if (schema.isOptional()) {
      return null;
    }
    throw new DataException("Missing mandatory value");
  }

  protected static Long castInt64(Object content, Schema schema) {
    if (content instanceof Byte) {
      return ((Byte) content).longValue();
    }
    if (content instanceof Short) {
      return ((Short) content).longValue();
    }
    if (content instanceof Integer) {
      return ((Integer) content).longValue();
    }
    if (content instanceof Long) {
      return (Long) content;
    }
    if (content instanceof Number) {
      // warn
      return ((Number) content).longValue();
    }
    if (content != null) {
      throw new DataException("Cannot cast "+content.getClass()+" to INT64");
    }
    if (schema.defaultValue() != null) {
      return (Long) schema.defaultValue();
    }
    if (schema.isOptional()) {
      return null;
    }
    throw new DataException("Missing mandatory value");
  }

  protected static Boolean castBoolean(Object content, Schema schema) {
    if (content instanceof Boolean) {
      return (Boolean) content;
    }
    if (content != null) {
      throw new DataException("Cannot cast "+content.getClass()+" to BOOLEAN");
    }
    if (schema.defaultValue() != null) {
      return (Boolean) schema.defaultValue();
    }
    if (schema.isOptional()) {
      return null;
    }
    throw new DataException("Missing mandatory value");
  }

  protected static Float castFloat32(Object content, Schema schema) {
    if (content instanceof Number) {
      return ((Number) content).floatValue();
    }
    if (content != null) {
      throw new DataException("Cannot cast "+content.getClass()+" to FLOAT32");
    }
    if (schema.defaultValue() != null) {
      return (Float) schema.defaultValue();
    }
    if (schema.isOptional()) {
      return null;
    }
    throw new DataException("Missing mandatory value");
  }

  protected static Double castFloat64(Object content, Schema schema) {
    if (content instanceof Number) {
      return ((Number) content).doubleValue();
    }
    if (content != null) {
      throw new DataException("Cannot cast "+content.getClass()+" to FLOAT64");
    }
    if (schema.defaultValue() != null) {
      return (Double) schema.defaultValue();
    }
    if (schema.isOptional()) {
      return null;
    }
    throw new DataException("Missing mandatory value");
  }

  protected static String castString(Object content, Schema schema) {
    if (content instanceof String) {
      return (String) content;
    }
    if (content != null) {
      throw new DataException("Cannot cast "+content.getClass()+" to STRING");
    }
    if (schema.defaultValue() != null) {
      return (String) schema.defaultValue();
    }
    if (schema.isOptional()) {
      return null;
    }
    throw new DataException("Missing mandatory value");
  }

  protected static byte[] castBytes(Object content, Schema schema) {
    if (content instanceof byte[]) {
      return (byte[]) content;
    }
    if (content instanceof String) {
      return ((String) content).getBytes(StandardCharsets.US_ASCII);
    }
    if (content != null) {
      throw new DataException("Cannot cast "+content.getClass()+" to BYTES");
    }
    if (schema.defaultValue() != null) {
      return (byte[]) schema.defaultValue();
    }
    if (schema.isOptional()) {
      return null;
    }
    throw new DataException("Missing mandatory value");
  }

  protected static List<?> castArray(Object content, Schema schema) {
    if (content instanceof Object[]) {
      return Stream.of((Object[]) content)
              .map(x->castTo(schema.valueSchema()))
              .collect(Collectors.toList());
    }
    if (content instanceof Collection) {
      return ((Collection<?>)content).stream()
              .map(x->castTo(schema.valueSchema()))
              .collect(Collectors.toList());
    }
    if (content != null) {
      throw new DataException("Cannot cast "+content.getClass()+" to ARRAY");
    }
    if (schema.defaultValue() != null) {
      return (List<?>) schema.defaultValue();
    }
    if (schema.isOptional()) {
      return null;
    }
    throw new DataException("Missing mandatory value");
  }

  protected static Map<?, ?> castMap(Object content, Schema schema) {
    if (content instanceof Map) {
      return ((Map<?,?>)content).entrySet().stream()
              .collect(Collectors.toMap(
                      castTo(schema.keySchema()),
                      castTo(schema.valueSchema())
              ));
    }
    if (content != null) {
      throw new DataException("Cannot cast "+content.getClass()+" to MAP");
    }
    if (schema.defaultValue() != null) {
      return (Map<?, ?>) schema.defaultValue();
    }
    if (schema.isOptional()) {
      return null;
    }
    throw new DataException("Missing mandatory value");
  }

  protected static Struct castStruct(Object content, Schema schema) {
    if (content instanceof Map) {
      Struct struct = new Struct(schema);
      for (Field field : schema.fields()) {
        struct.put(field, castTo(field.schema()).apply(((Map<?,?>)content).get(field.name())));
      }
      for (Map.Entry<?, ?> e : ((Map<?,?>)content).entrySet()) {
        if (!(e.getValue() instanceof String)) {
          //
          continue;
        }
        if (schema.field((String)e.getValue()) == null) {
          //warn
          continue;
        }
      }
      return struct;
    }
    if (content instanceof Struct) {
      Struct struct = new Struct(schema);
      for (Field field : schema.fields()) {
        Object fieldValue;
        if (((Struct)content).schema().field(field.name()) != null) {
          fieldValue = ((Struct)content).get(field.name());
        } else {
          fieldValue = null;
        }
        struct.put(field, castTo(field.schema()).apply(fieldValue));
      }
      for (Field field : ((Struct)content).schema().fields()) {
        if (schema.field(field.name()) == null) {
          //warn
          continue;
        }
      }
      return struct;
    }
    if (content != null) {
      throw new DataException("Cannot cast "+content.getClass()+" to STRUCT");
    }
    if (schema.defaultValue() != null) {
      return (Struct) schema.defaultValue();
    }
    if (schema.isOptional()) {
      return null;
    }
    throw new DataException("Missing mandatory value");
  }

  protected static Function<Object, Object> castTo(Schema schema) {
    BiFunction<Object, Schema, ? extends Object> conversion = conversions.get(schema.type());
    if (conversion == null) {
      throw new IllegalArgumentException("Schema conversion not supported: " + schema.type());
    }
    return x->conversion.apply(x, schema);
  }

}
