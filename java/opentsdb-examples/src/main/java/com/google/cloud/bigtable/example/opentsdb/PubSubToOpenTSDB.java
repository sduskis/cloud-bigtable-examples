package com.google.cloud.bigtable.example.opentsdb;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import com.google.common.collect.ImmutableMap;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;
import com.google.cloud.bigtable.example.opentsdb.Util.TimeSeriesPoint;

public class PubSubToOpenTSDB {

  static final Logger LOG = LoggerFactory.getLogger(PubSubToOpenTSDB.class);

  private static DoFn<String, TimeSeriesPoint> TO_TS_POINT = new DoFn<String, TimeSeriesPoint>() {
    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void ProcessElement(ProcessContext context) {
      String message = context.element();
      try {
        TimeSeriesPoint p = toTimeSeriesPoint(message);
        if (p != null) {
          Log.info("adding " + p);
          context.output(p);
        } else {
          Log.info("Could not parse  " + message);
        }
      } catch (Exception e) {
        LOG.warn("Could not parse the message: " + message, e);
      }
    }

    // the message format is something like:
    // my-registry/my-device/temperature/75.7294657654/2017-09-27 16:21:29.133225
    private TimeSeriesPoint toTimeSeriesPoint(String message)
        throws NumberFormatException, ParseException {
      String[] values = message.split("/");
      if (values.length == 5) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return new TimeSeriesPoint(values[2], // metric
            sdf.parse(values[4]).getTime(), // time
            Double.valueOf(values[3]), // value
            ImmutableMap.of("deviceId", values[1])); // attributes
      }
      throw new RuntimeException("Message didn't have 5 '/'");
    }
  };

  public static void main(String[] args) throws NumberFormatException, ParseException {
    DataflowPipelineOptions options = Util.createOptions();

    options.setAppName("DumpPubSubMessages");

    Pipeline p = Pipeline.create(options);

    final String subscription = "projects/[PROJECT_ID]/subscriptions/[SUBSCRIPTION_ID]";
    p
       .apply(PubsubIO.readStrings().fromSubscription(subscription))
       .apply("Create Time Series", ParDo.of(TO_TS_POINT))
       .apply("Write to OpenTSDB", ParDo.of(Util.WRITE_TIMESERIES));

    p.run().waitUntilFinish();
  }
}
