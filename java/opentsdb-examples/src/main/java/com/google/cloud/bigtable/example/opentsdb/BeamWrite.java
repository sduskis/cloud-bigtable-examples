package com.google.cloud.bigtable.example.opentsdb;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import net.opentsdb.core.TSDB;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import com.google.cloud.bigtable.example.opentsdb.Util.TimeSeriesPoint;
import com.google.common.collect.ImmutableMap;

public class BeamWrite {

  public static DoFn<String, TimeSeriesPoint> GENERATE_TEST_DATA = new DoFn<String, TimeSeriesPoint>() {
    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(ProcessContext context) {
      String metric = context.element();
      long start = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1000);
      Map<String, String> attributes = ImmutableMap.of("Foo", "bar");
      for (long i = 0; i < 1000; i += 1) {
        double value = Math.random() * 100;
        long time =  start + (TimeUnit.MINUTES.toMillis(i));
        context.output(new TimeSeriesPoint(metric, time, value, attributes));
      }
    }
  };

  public static void main(String[] args) throws IOException {
    DataflowPipelineOptions options = Util.createOptions();
    options.setAppName("OpenTSDBTest");

    Pipeline p = Pipeline.create(options);

    List<String> metrics = Arrays.asList("metric1", "metric2");

    TSDB tsdb = Util.getTsdb();
    for(String metric : metrics) {
      tsdb.assignUid("metric", metric);
    }

    p
      .apply("Keys", Create.of(metrics))
      .apply("Create Time Series", ParDo.of(GENERATE_TEST_DATA))
      .apply("Write to ", ParDo.of(Util.WRITE_TIMESERIES));

    p.run().waitUntilFinish();
  }
}
