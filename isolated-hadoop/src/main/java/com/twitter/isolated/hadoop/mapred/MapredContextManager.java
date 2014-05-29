package com.twitter.isolated.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.twitter.isolated.hadoop.ContextManager;
import com.twitter.isolated.hadoop.InputSpec;

public class MapredContextManager extends ContextManager {

  MapredContextManager(JobConf conf) {
    super(conf);
  }

  // methods that make sure delegated calls are executed in the right context

  public InputSplit[] getSplits(final int numSplits) throws IOException {
    List<IsolatedInputSplit> result = new ArrayList<IsolatedInputSplit>();
    for (final InputSpec inputSpec : super.getInputSpecs()) {
      result.addAll(callInContext(inputSpec, new MapredContextualCall<List<IsolatedInputSplit>>() {
        @Override
        public List<IsolatedInputSplit> call(JobConf contextualConf) throws IOException, InterruptedException {
          InputFormat<?, ?> inputFormat = newInputFormat(contextualConf, inputSpec, InputFormat.class);
          List<IsolatedInputSplit> finalSplits = new ArrayList<IsolatedInputSplit>();
          for (InputSplit inputSplit : inputFormat.getSplits(contextualConf, numSplits)) {
            finalSplits.add(new IsolatedInputSplit(inputSpec.getId(), inputSplit, new JobConf(conf)));
          }
          return finalSplits;
        }
      }));
    }
    return result.toArray(new InputSplit[result.size()]);
  }

  public <K, V> RecordReader<K, V> getRecordReader(InputSplit split, final Reporter reporter) throws IOException {
    final IsolatedInputSplit isolatedSplit = (IsolatedInputSplit)split;
    final InputSpec inputSpec = getInputSpec(isolatedSplit.getInputSpecID());
    return callInContext(inputSpec, new MapredContextualCall<RecordReader<K, V>>() {
      public RecordReader<K, V> call(JobConf contextualConf) throws IOException, InterruptedException {
        @SuppressWarnings("unchecked") // wishful thinking
        InputFormat<K, V> inputFormat = newInputFormat(contextualConf, inputSpec, InputFormat.class);
        return inputFormat.getRecordReader(isolatedSplit.getDelegate(), contextualConf, reporter);
      }
    });
  }

  private static abstract class MapredContextualCall<T> extends ContextualCall<T> {

    abstract public T call(JobConf contextualConf) throws IOException, InterruptedException;

    @Override
    public final T call(Configuration contextualConf) throws IOException, InterruptedException {
      JobConf jobConf = new JobConf(contextualConf);
      try {
        return call(jobConf);
      } finally {
        setAfterConfiguration(jobConf);
      }
    }

  }
}
