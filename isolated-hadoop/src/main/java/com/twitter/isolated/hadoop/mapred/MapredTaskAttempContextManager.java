package com.twitter.isolated.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

class MapredTaskAttempContextManager extends MapredContextManager {

  private final TaskAttemptContext globalTaskContext;

  MapredTaskAttempContextManager(TaskAttemptContext globalTaskContext) {
    super(globalTaskContext.getJobConf());
    this.globalTaskContext = globalTaskContext;
  }

  void setupTask(final OutputCommitter delegate) throws IOException {
    callInContext(getOutputSpec(),  new TaskContextualRun() {
      public void run(TaskCallContext ctxt) throws IOException, InterruptedException {
        delegate.setupTask(ctxt.localTaskContext);
      }
    });
  }

  boolean needsTaskCommit(final OutputCommitter delegate) throws IOException {
    return callInContext(getOutputSpec(),  new TaskContextualCall<Boolean>() {
      public Boolean call(TaskCallContext ctxt) throws IOException, InterruptedException {
        return delegate.needsTaskCommit(ctxt.localTaskContext);
      }
    });
  }

  void commitTask(final OutputCommitter delegate) throws IOException {
    callInContext(getOutputSpec(),  new TaskContextualRun() {
      public void run(TaskCallContext ctxt) throws IOException, InterruptedException {
        delegate.commitTask(ctxt.localTaskContext);
      }
    });
  }

  void abortTask(final OutputCommitter delegate) throws IOException {
    callInContext(getOutputSpec(),  new TaskContextualRun() {
      public void run(TaskCallContext ctxt) throws IOException, InterruptedException {
        delegate.abortTask(ctxt.localTaskContext);
      }
    });
  }

  private static class TaskCallContext extends CallContext {

    private final org.apache.hadoop.mapreduce.TaskAttemptContext localTaskContext;

    public TaskCallContext(CallContext ctxt, org.apache.hadoop.mapreduce.TaskAttemptContext localTaskContext) {
      super(ctxt, localTaskContext.getConfiguration());
      this.localTaskContext = localTaskContext;
    }

  }

  private abstract class TaskContextualRun extends TaskContextualCall<Void> {

    abstract void run(TaskCallContext ctxt) throws IOException, InterruptedException;

    @Override
    final public Void call(TaskCallContext ctxt) throws IOException, InterruptedException {
      this.run(ctxt);
      return null;
    }
  }

  private abstract class TaskContextualCall<T> extends MapredContextualCall<T> {

    abstract T call(TaskCallContext ctxt) throws IOException, InterruptedException;

    @Override
    final public T call(MapredCallContext ctxt) throws IOException, InterruptedException {
      return this.call(
          new TaskCallContext(
              ctxt,
              new org.apache.hadoop.mapreduce.TaskAttemptContext(
                  ctxt.localJobConf,
                  globalTaskContext.getTaskAttemptID())));
    }

  }

}
