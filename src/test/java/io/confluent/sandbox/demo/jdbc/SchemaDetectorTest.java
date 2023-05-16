package io.confluent.sandbox.demo.jdbc;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;

public class SchemaDetectorTest {

  @Test
  void testDetectRecord() {

    SchemaDetector detector = new SchemaDetector();

    Map<String, Object> record = new LinkedHashMap<>();

    record.put("id", "test");
    record.put("value", 20);
    record.put("name", "Jacob");

    Schema schema = detector.detectSchema(null, record);

    assertThat(schema).satisfies(
            s->assertThat(schema.type()).isEqualTo(Schema.Type.STRUCT),
            s->assertThat(schema.isOptional()).isTrue(),
            s->assertThat(schema.fields()).hasSize(3).satisfies(
                    fields -> assertThat(fields.get(0))
                            .describedAs("First field is 'id' and is optional String")
                            .satisfies(
                                    f -> assertThat(f.name()).isEqualTo("id"),
                                    f -> assertThat(f.schema().type()).isEqualTo(Schema.Type.STRING),
                                    f -> assertThat(f.schema().isOptional()).isTrue()
                            ),
                    fields -> assertThat(fields.get(1))
                            .describedAs("Second field is 'value' and is optional Int32")
                            .satisfies(
                                    f -> assertThat(f.name()).isEqualTo("value"),
                                    f -> assertThat(f.schema().type()).isEqualTo(Schema.Type.INT32),
                                    f -> assertThat(f.schema().isOptional()).isTrue()
                            ),
                    fields -> assertThat(fields.get(2))
                            .describedAs("Third field is 'name' and is optional String")
                            .satisfies(
                                    f -> assertThat(f.name()).isEqualTo("name"),
                                    f -> assertThat(f.schema().type()).isEqualTo(Schema.Type.STRING),
                                    f -> assertThat(f.schema().isOptional()).isTrue()
                            )
            )
    );

  }

}
