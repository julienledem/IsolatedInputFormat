package com.twitter.isolated.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ConfigModifierInputFormat extends InputFormat<String, String>{

  public static final class DummySplit extends InputSplit implements Writable {

    @Override
    public long getLength() throws IOException, InterruptedException {
      return 10;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
    }
  }

  @Override
  public RecordReader<String, String> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    return new RecordReader<String, String>() {

      String key;
      String value;
      boolean hasNext;

      @Override
      public void close() throws IOException {
      }

      @Override
      public String getCurrentKey() throws IOException, InterruptedException {
        return key;
      }

      @Override
      public String getCurrentValue() throws IOException, InterruptedException {
        return value;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return hasNext ? 0 : 1;
      }

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context)
          throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.key = "key:" + configuration.get("my.external.key");
        this.value = "value:" + configuration.get("my.internal.key");
        this.hasNext = true;
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        try {
          return hasNext;
        } finally {
          hasNext = false;
        }
      }
    };
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    Configuration configuration = context.getConfiguration();
    String v = configuration.get("my.external.key");
    System.out.println("ConfigModifierInputFormat set(\"my.internal.key\", \"" + v + "\")");
    configuration.set("my.internal.key", v);
    return Arrays.<InputSplit>asList(new DummySplit());
  }



}
