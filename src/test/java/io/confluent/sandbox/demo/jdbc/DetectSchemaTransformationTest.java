package io.confluent.sandbox.demo.jdbc;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.confluent.sandbox.demo.jdbc.Assertions.assertThat;
import static io.confluent.sandbox.demo.jdbc.JacksonUtils.yaml2json;
import static io.confluent.sandbox.demo.jdbc.ResourceUtils.readResource;

public class DetectSchemaTransformationTest {

  @Test
  void testSchemaDetection() {

    DetectSchemaTransformation<SinkRecord> transformation = new DetectSchemaTransformation.Value<>();
    transformation.configure(Map.ofEntries(
            // empty
    ));

    SinkRecord inputRecord = new SinkRecord(
            "test-topic",
            0,
            Schema.OPTIONAL_STRING_SCHEMA, // this comes from StringConverter
            "test",
            null, // JsonConverter emits null when envelope is not used
            Map.ofEntries(
                    Map.entry("USER_ID", "test-user"),
                    Map.entry("FIRST_NAME", "Test"),
                    Map.entry("LAST_NAME", "Test")
            ),
            0L
    );

    SinkRecord outputRecord = transformation.apply(inputRecord);

    assertThat(outputRecord).satisfies(
      r->assertThat(r.keySchema()).isEqualTo(Schema.OPTIONAL_STRING_SCHEMA),
      r->assertThat(r.key()).isEqualTo("test"),
      r->assertThat(r.valueSchema())
              .isNotNull()
              .satisfies(
                      s->assertThat(s.type())
                              .isEqualTo(Schema.Type.STRUCT),
                      s->assertThat(s.field("USER_ID"))
                              .satisfies(
                                      f->assertThat(f.schema().type()).isEqualTo(Schema.Type.STRING)
                              ),
                      s->assertThat(s.field("FIRST_NAME"))
                              .satisfies(
                                      f->assertThat(f.name()).isEqualTo("FIRST_NAME"),
                                      f->assertThat(f.schema().type()).isEqualTo(Schema.Type.STRING)
                              ),
                      s->assertThat(s.field("LAST_NAME"))
                              .satisfies(
                                      f->assertThat(f.name()).isEqualTo("LAST_NAME"),
                                      f->assertThat(f.schema().type()).isEqualTo(Schema.Type.STRING)
                              )
              ),
      r->assertThat(r.value()).isInstanceOfSatisfying(Struct.class,
              x->assertThat(x)
                      .satisfies(
                              s->assertThat(s.schema().type()).isEqualTo(Schema.Type.STRUCT),
                              s->assertThat(s.get("USER_ID")).isEqualTo("test-user"),
                              s->assertThat(s.get("FIRST_NAME")).isEqualTo("Test"),
                              s->assertThat(s.get("LAST_NAME")).isEqualTo("Test")
                      )
              )
    );

  }

}
