package com.twitter.isolated.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class IsolatedOutputFormat<K, V> extends OutputFormat<K, V> {

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException,
      InterruptedException {
    new MapreduceContextManager(context).checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new MapreduceContextManager(context).getOutputCommitter(context);
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new MapreduceContextManager(context).getRecordWriter(context);
  }

}
