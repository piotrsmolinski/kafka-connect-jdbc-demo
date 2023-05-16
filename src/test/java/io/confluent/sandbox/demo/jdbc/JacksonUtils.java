package io.confluent.sandbox.demo.jdbc;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JacksonUtils {

  public static <T> T parseYaml(String yaml, Class<T> targetClass) {
    return parseYaml(yaml.getBytes(StandardCharsets.UTF_8), targetClass);
  }

  public static <T> T parseYaml(byte[] yaml, Class<T> targetClass) {
    ObjectMapper yamlOM = new ObjectMapper(new YAMLFactory());
    try {
      return yamlOM.readValue(yaml, targetClass);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T parseYaml(String yaml, TypeReference<T> targetClass) {
    return parseYaml(yaml.getBytes(StandardCharsets.UTF_8), targetClass);
  }

  public static <T> T parseYaml(byte[] yaml, TypeReference<T> targetClass) {
    ObjectMapper yamlOM = new ObjectMapper(new YAMLFactory());
    try {
      return yamlOM.readValue(yaml, targetClass);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String renderJson(Object object) {
    ObjectMapper jsonOM = new ObjectMapper(new JsonFactory());
    try {
      return jsonOM.writeValueAsString(object);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T parseJson(String json, Class<T> targetClass) {
    return parseJson(json.getBytes(StandardCharsets.UTF_8), targetClass);
  }

  public static <T> T parseJson(byte[] json, Class<T> targetClass) {
    ObjectMapper jsonOM = new ObjectMapper(new JsonFactory());
    try {
      return jsonOM.readValue(json, targetClass);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T parseJson(String json, TypeReference<T> targetClass) {
    return parseJson(json.getBytes(StandardCharsets.UTF_8), targetClass);
  }

  public static <T> T parseJson(byte[] json, TypeReference<T> targetClass) {
    ObjectMapper jsonOM = new ObjectMapper(new JsonFactory());
    try {
      return jsonOM.readValue(json, targetClass);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static TypeReference<Map<String,Object>> asConfig() {
    return new TypeReference<>() {};
  }
  public static String yaml2json(String yaml) {
    return renderJson(parseYaml(yaml, Object.class));
  }

  public static byte[] yaml2json(byte[] yaml) {
    return yaml2json(new String(yaml, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
  }

}
