package io.confluent.sandbox.demo.jdbc;

import org.assertj.db.type.Request;
import org.assertj.db.type.Source;

public class DatabaseUtils {

  public static Request waitUntilPresent(long timeout, Source source, String query, Object... parameters) {
    long deadline = System.currentTimeMillis() + timeout;
    for (;;) {
      Request request = new Request(source, query, parameters);
      if (!request.getRowsList().isEmpty()) {
        return request;
      }
      long remaining = deadline - System.currentTimeMillis();
      if (remaining <= 0) {
        return request;
      }
      try {
        Thread.sleep(Math.min(100L, remaining));
      } catch (InterruptedException e) {
        return request;
      }
    }
  }

}
