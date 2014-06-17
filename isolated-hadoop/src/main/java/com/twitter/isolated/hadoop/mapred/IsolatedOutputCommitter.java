package com.twitter.isolated.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

final class IsolatedOutputCommitter<T> extends OutputCommitter {
  private final OutputCommitter delegate;

  IsolatedOutputCommitter(OutputCommitter delegate) {
    this.delegate = delegate;
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    new MapredTaskAttempContextManager(context).setupTask(delegate);
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    new MapredJobContextManager(context).setupJob(delegate);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    return new MapredTaskAttempContextManager(context).needsTaskCommit(delegate);
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    new MapredTaskAttempContextManager(context).commitTask(delegate);
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    new MapredTaskAttempContextManager(context).abortTask(delegate);
  }
}