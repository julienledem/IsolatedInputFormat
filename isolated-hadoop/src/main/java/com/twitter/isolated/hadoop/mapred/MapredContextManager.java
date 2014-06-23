package com.twitter.isolated.hadoop.mapred;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import com.twitter.isolated.hadoop.ContextManager;
import com.twitter.isolated.hadoop.Spec;

class MapredContextManager extends ContextManager {

  MapredContextManager(JobConf conf) {
    super(conf);
  }

  // methods that make sure delegated calls are executed in the right context

  // input format

  InputSplit[] getSplits(final int numSplits) throws IOException {
    List<IsolatedInputSplit> result = new ArrayList<IsolatedInputSplit>();
    for (final Spec inputSpec : super.getInputSpecs()) {
      result.addAll(callInContext(inputSpec, new MapredContextualCall<List<IsolatedInputSplit>>() {
        @Override
        public List<IsolatedInputSplit> call(MapredCallContext context) throws IOException, InterruptedException {
          InputFormat<?, ?> inputFormat = context.newInstanceFromSpec(InputFormat.class);
          List<IsolatedInputSplit> finalSplits = new ArrayList<IsolatedInputSplit>();
          for (InputSplit inputSplit : inputFormat.getSplits(context.localJobConf, numSplits)) {
            finalSplits.add(new IsolatedInputSplit(context.spec.getId(), inputSplit, new JobConf(globalConf)));
          }
          return finalSplits;
        }
      }));
    }
    return result.toArray(new InputSplit[result.size()]);
  }

  <K, V> RecordReader<K, V> getRecordReader(InputSplit split, final Reporter reporter) throws IOException {
    final IsolatedInputSplit isolatedSplit = (IsolatedInputSplit)split;
    final Spec inputSpec = getSpec(isolatedSplit.getInputSpecID());
    return callInContext(inputSpec, new MapredContextualCall<RecordReader<K, V>>() {
      public RecordReader<K, V> call(MapredCallContext context) throws IOException, InterruptedException {
        @SuppressWarnings("unchecked") // wishful thinking
        InputFormat<K, V> inputFormat = context.newInstanceFromSpec(InputFormat.class);
        return inputFormat.getRecordReader(isolatedSplit.getDelegate(), context.localJobConf, reporter);
      }
    });
  }


  // output format

  <K, V> void checkOutputSpecs(final FileSystem ignored) throws IOException {
    callInContext(getOutputSpec(), new MapredContextualRun() {
      public void run(MapredCallContext context) throws IOException, InterruptedException {
        @SuppressWarnings("unchecked") // wishful thinking
        OutputFormat<K, V> outputFormat = context.newInstanceFromSpec(OutputFormat.class);
        outputFormat.checkOutputSpecs(ignored, context.localJobConf);
      }
    });
  }

  <K, V> RecordWriter<K, V> getRecordWriter(final FileSystem ignored, final String name, final Progressable p) throws IOException {
    return callInContext(getOutputSpec(), new MapredContextualCall<RecordWriter<K, V>>() {
      public RecordWriter<K, V> call(MapredCallContext ctxt) throws IOException, InterruptedException {
        @SuppressWarnings("unchecked") // wishful thinking
        OutputFormat<K, V> outputFormat = ctxt.newInstanceFromSpec(OutputFormat.class);
        return outputFormat.getRecordWriter(ignored, ctxt.localJobConf, name, p);
      }
    });
  }

  InputSplit readSplit(String inputSpecID, final String className, final DataInput input) throws IOException {
    return callInContext(inputSpecID, new ContextualCall<InputSplit>() {
      @Override
      public InputSplit call(CallContext ctxt) throws IOException, InterruptedException {
        InputSplit delegate = ctxt.newInstance(className, InputSplit.class);
        delegate.readFields(input);
        return delegate;
      }
    });
  }
  // MapRed specific context management

  static class MapredCallContext extends CallContext {

    final JobConf localJobConf;

    MapredCallContext(CallContext ctxt, JobConf localJobConf) {
      super(ctxt, localJobConf);
      this.localJobConf = localJobConf;
    }

  }

  static abstract class MapredContextualCall<T> extends ContextualCall<T> {

    abstract T call(MapredCallContext ctxt) throws IOException, InterruptedException;

    @Override
    public final T call(CallContext ctxt) throws IOException, InterruptedException {
      JobConf localJobConf = new JobConf(ctxt.localConf()) {
        @Override
        public OutputCommitter getOutputCommitter() {
          return new IsolatedOutputCommitter<T>(super.getOutputCommitter());
        }
      };
      return call(new MapredCallContext(ctxt, localJobConf));
    }

  }

  static abstract class MapredContextualRun extends MapredContextualCall<Void> {

    @Override
    final public Void call(MapredCallContext ctxt) throws IOException, InterruptedException {
      run(ctxt);
      return null;
    }

    abstract void run(MapredCallContext ctxt) throws IOException, InterruptedException;

  }

}
