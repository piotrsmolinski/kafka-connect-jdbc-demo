package io.confluent.sandbox.demo.jdbc;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.confluent.sandbox.demo.jdbc.Assertions.*;
import static io.confluent.sandbox.demo.jdbc.JacksonUtils.*;
import static io.confluent.sandbox.demo.jdbc.ResourceUtils.*;

public class CastSchemaTransformationTest {

  @Test
  void testSchemaCasting() {

    CastSchemaTransformation<SinkRecord> transformation = new CastSchemaTransformation.Value<>();
    transformation.configure(Map.ofEntries(
            Map.entry("schema.url", "text:"+new String(yaml2json(readResource("schemas/user.avsc"))))
    ));

    SinkRecord inputRecord = new SinkRecord(
            "test-topic",
            0,
            null,
            "test",
            null,
            Map.ofEntries(
                    Map.entry("USER_ID", "test-user"),
                    Map.entry("FIRST_NAME", "Test"),
                    Map.entry("LAST_NAME", "Test")
            ),
            0L
    );

    SinkRecord outputRecord = transformation.apply(inputRecord);

    assertThat(outputRecord).satisfies(
      r->assertThat(r.keySchema()).isNull(),
      r->assertThat(r.key()).isEqualTo("test"),
      r->assertThat(r.valueSchema()).isNotNull(),
      r->assertThat(r.value()).isInstanceOf(Struct.class)
    );

  }

}
