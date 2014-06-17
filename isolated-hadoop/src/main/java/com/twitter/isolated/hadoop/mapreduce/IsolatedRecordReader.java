package com.twitter.isolated.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

final class IsolatedRecordReader<K, V> extends RecordReader<K, V> {
  private final RecordReader<K, V> delegate;

  IsolatedRecordReader(RecordReader<K, V> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void close() throws IOException {
    delegate.close();

  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return delegate.getCurrentKey();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return delegate.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return delegate.getProgress();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    new MapreduceTaskAttemptContextManager(context).initializeRecordReader(delegate, split);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return delegate.nextKeyValue();
  }
}