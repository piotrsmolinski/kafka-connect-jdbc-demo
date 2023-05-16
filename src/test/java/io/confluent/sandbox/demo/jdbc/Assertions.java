package io.confluent.sandbox.demo.jdbc;

import org.assertj.db.api.ChangesAssert;
import org.assertj.db.api.RequestAssert;
import org.assertj.db.api.TableAssert;
import org.assertj.db.type.Changes;
import org.assertj.db.type.Request;
import org.assertj.db.type.Table;

/**
 * Extension of the base AssertJ assertions that adds assertj-db API.
 */
public class Assertions extends org.assertj.core.api.Assertions {

  protected Assertions() {
  }

  public static TableAssert assertThat(Table table) {
    return org.assertj.db.api.Assertions.assertThat(table);
  }

  public static RequestAssert assertThat(Request request) {
    return org.assertj.db.api.Assertions.assertThat(request);
  }

  public static ChangesAssert assertThat(Changes changes) {
    return org.assertj.db.api.Assertions.assertThat(changes);
  }


}
