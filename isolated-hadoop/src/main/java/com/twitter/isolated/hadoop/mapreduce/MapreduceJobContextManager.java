package com.twitter.isolated.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;

import com.twitter.isolated.hadoop.Spec;

class MapreduceJobContextManager extends MapreduceContextManager {

  private final JobContext globalJobContext;

  MapreduceJobContextManager(JobContext globalJobContext) {
    super(globalJobContext.getConfiguration());
    this.globalJobContext = globalJobContext;
  }

  List<InputSplit> getSplits() throws IOException {
    final List<InputSplit> finalSplits = new ArrayList<InputSplit>();
    for (final Spec inputSpec : getInputSpecs()) {
      callInContext(inputSpec, new JobContextualRun() {
        public void run(JobCallContext ctxt) throws IOException, InterruptedException {
          InputFormat<?, ?> inputFormat = ctxt.newInstanceFromSpec(InputFormat.class);
          List<InputSplit> splits = inputFormat.getSplits(ctxt.localJobContext);
          for (InputSplit inputSplit : splits) {
            finalSplits.add(new IsolatedInputSplit(ctxt.spec.getId(), inputSplit, globalConf));
          }
        }
      });
    }
    return finalSplits;
  }

  void checkOutputSpecs() throws IOException {
    callInContext(getOutputSpec(), new JobContextualRun() {
      @Override
      void run(JobCallContext ctxt) throws IOException, InterruptedException {
        OutputFormat<?, ?> outputFormat = ctxt.newInstanceFromSpec(OutputFormat.class);
        outputFormat.checkOutputSpecs(ctxt.localJobContext);
      }
    });
  }

  private static class JobCallContext extends CallContext {

    private JobContext localJobContext;

    public JobCallContext(CallContext ctxt, JobContext localJobContext) {
      super(ctxt, localJobContext.getConfiguration());
      this.localJobContext = localJobContext;
    }

  }

  private abstract class JobContextualRun extends ContextualCall<Void> {

    abstract void run(JobCallContext ctxt) throws IOException, InterruptedException;

    @Override
    final public Void call(CallContext ctxt) throws IOException, InterruptedException {
      this.run(
          new JobCallContext(
              ctxt,
              new JobContext(
                  ctxt.localConf(),
                  globalJobContext.getJobID())));
      return null;
    }
  }
}
