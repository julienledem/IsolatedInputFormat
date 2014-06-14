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
    new MapredContextManager(context.getJobConf()).setupTask(delegate, context);
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    new MapredContextManager(context.getJobConf()).setupJob(delegate, context);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    return new MapredContextManager(context.getJobConf()).needsTaskCommit(delegate, context);
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    new MapredContextManager(context.getJobConf()).commitTask(delegate, context);
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    new MapredContextManager(context.getJobConf()).abortTask(delegate, context);
  }
}