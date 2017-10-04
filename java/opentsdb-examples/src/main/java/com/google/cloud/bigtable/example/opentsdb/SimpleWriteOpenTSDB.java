package com.google.cloud.bigtable.example.opentsdb;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

public class SimpleWriteOpenTSDB {

  static final String metric = "fooMetric";

  public static void main(String[] args) throws IOException {
    String file = Util.extractToFile("/opentsdb.conf");
    Config config = new Config(file);
    TSDB tsdb = new TSDB(config);
    write(tsdb);
  }

  protected static void write(TSDB tsdb) {
    long diffUnit = TimeUnit.SECONDS.toMillis(5);
    long start = System.currentTimeMillis() - (diffUnit * 1000);
    System.out.println("Start");
    Map<String, String> map = ImmutableMap.of("Foo", "bar");
    for (long i = 0; i < 1000; i += 1) {
      double value = Math.random() * 100;
      tsdb.addPoint(metric, start + (i * diffUnit), value, map);
    }
    System.out.println("Flushing");
    tsdb.flush();
    System.out.println("Flushed");
  }
}
