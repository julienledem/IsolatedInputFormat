package com.twitter.isolated.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;

class MapredJobContextManager extends MapredContextManager {

  private final JobContext globalJobContext;

  MapredJobContextManager(JobContext globalJobContext) {
    super(globalJobContext.getJobConf());
    this.globalJobContext = globalJobContext;
  }

  void setupJob(final OutputCommitter delegate) throws IOException {
    callInContext(getOutputSpec(),  new JobContextualRun() {
      public void run(JobCallContext ctxt) throws IOException, InterruptedException {
        delegate.setupJob(ctxt.localJobContext);
      }
    });
  }

  private static class JobCallContext extends CallContext {

    private final org.apache.hadoop.mapreduce.JobContext localJobContext;

    public JobCallContext(CallContext ctxt, org.apache.hadoop.mapreduce.JobContext localJobContext) {
      super(ctxt, localJobContext.getConfiguration());
      this.localJobContext = localJobContext;
    }

  }

  private abstract class JobContextualRun extends MapredContextualRun {

    abstract void run(JobCallContext ctxt) throws IOException, InterruptedException;

    @Override
    final public void run(MapredCallContext ctxt) throws IOException, InterruptedException {
      this.run(new JobCallContext(
          ctxt,
          new org.apache.hadoop.mapreduce.JobContext(
              ctxt.localJobConf,
              globalJobContext.getJobID())));
    }
  }
}
