package com.twitter.isolated.hadoop.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class IsolatedInputFormat<K, V> extends InputFormat<K, V> {

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return new MapreduceJobContextManager(context).getSplits();
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new IsolatedRecordReader<K, V>(new MapreduceTaskAttemptContextManager(context).<K, V>createRecordReader(split));
  }

}
