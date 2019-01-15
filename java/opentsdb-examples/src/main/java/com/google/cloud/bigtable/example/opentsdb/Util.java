package com.google.cloud.bigtable.example.opentsdb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

public class Util {
  static final Logger LOG = LoggerFactory.getLogger(Util.class);

  public static class TimeSeriesPoint implements Serializable {
    private static final long serialVersionUID = 1L;
    final String metric;
    final long time;
    final double value;
    final Map<String, String> attributes;

    public TimeSeriesPoint(String metric, long time, double value, Map<String, String> attributes) {
      this.metric = metric;
      this.time = time;
      this.value = value;
      this.attributes = attributes;
    }

    @Override
    public String toString() {
      return "TimeSeriesPoint [metric=" + metric + ", time=" + time + ", value=" + value
          + ", attributes=" + attributes + "]";
    }
  }

  transient static TSDB tsdb;

  protected static TSDB getTsdb() throws IOException {
    if (tsdb == null) {
      String file = Util.extractToFile("/opentsdb.conf");
      LOG.info("Initializing tsdb");
      tsdb = new TSDB(new Config(file));
    }
    return tsdb;
  }

  public static DoFn<TimeSeriesPoint, Void> WRITE_TIMESERIES = new DoFn<TimeSeriesPoint, Void>() {
    private static final long serialVersionUID = 1L;
    transient TSDB tsdb;

    @Setup
    public synchronized void startBundle() throws IOException {
      tsdb = getTsdb();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      TimeSeriesPoint point = context.element();
      tsdb.addPoint(point.metric, point.time, point.value, point.attributes);
    }

    @Teardown
    public void finish() {
      if (tsdb != null) {
        LOG.info("Flushing tsdb");
        tsdb.flush();
      }
    }
  };

  static DataflowPipelineOptions createOptions() {
    String stagingLocation = "[YOUR BUCKET]";
    String args[] = new String[]{
        "--project", "[Your project ID]",
        "--region",  "[Your Dataflow region]",
        "--stagingLocation", stagingLocation + "/stage",
        "--tempLocation", stagingLocation + "/temp",
        "--runner=direct"
    };
    return PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
  }

  static public String extractToFile(String resourceName) throws IOException {
    try (InputStream stream = SimpleWriteOpenTSDB.class.getResourceAsStream(resourceName)){
      // note that each / is a directory down in the "jar tree" been the jar the root of the tree
      if (stream == null) {
        throw new IOException("Cannot get resource \"" + resourceName + "\" from Jar file.");
      }

      int readBytes;
      File tempFile = File.createTempFile("opentsdb-test", ".conf");
      try (OutputStream resStreamOut = new FileOutputStream(tempFile)) {
        byte[] buffer = new byte[4096];
        while ((readBytes = stream.read(buffer)) > 0) {
          resStreamOut.write(buffer, 0, readBytes);
        }
      }
      return tempFile.getCanonicalPath();
    }
  }

}
