package com.twitter.isolated.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import com.twitter.isolated.hadoop.ContextManager;
import com.twitter.isolated.hadoop.Spec;

public class MapredContextManager extends ContextManager {

  MapredContextManager(JobConf conf) {
    super(conf);
  }

  // methods that make sure delegated calls are executed in the right context

  // input format

  public InputSplit[] getSplits(final int numSplits) throws IOException {
    List<IsolatedInputSplit> result = new ArrayList<IsolatedInputSplit>();
    for (final Spec inputSpec : super.getInputSpecs()) {
      result.addAll(callInContext(inputSpec, new MapredContextualCall<List<IsolatedInputSplit>>() {
        @Override
        public List<IsolatedInputSplit> call(JobConf contextualConf) throws IOException, InterruptedException {
          InputFormat<?, ?> inputFormat = newInstanceFromSpec(contextualConf, spec, InputFormat.class);
          List<IsolatedInputSplit> finalSplits = new ArrayList<IsolatedInputSplit>();
          for (InputSplit inputSplit : inputFormat.getSplits(contextualConf, numSplits)) {
            finalSplits.add(new IsolatedInputSplit(spec.getId(), inputSplit, new JobConf(conf)));
          }
          return finalSplits;
        }
      }));
    }
    return result.toArray(new InputSplit[result.size()]);
  }

  public <K, V> RecordReader<K, V> getRecordReader(InputSplit split, final Reporter reporter) throws IOException {
    final IsolatedInputSplit isolatedSplit = (IsolatedInputSplit)split;
    final Spec inputSpec = getInputSpec(isolatedSplit.getInputSpecID());
    return callInContext(inputSpec, new MapredContextualCall<RecordReader<K, V>>() {
      public RecordReader<K, V> call(JobConf contextualConf) throws IOException, InterruptedException {
        @SuppressWarnings("unchecked") // wishful thinking
        InputFormat<K, V> inputFormat = newInstanceFromSpec(contextualConf, spec, InputFormat.class);
        return inputFormat.getRecordReader(isolatedSplit.getDelegate(), contextualConf, reporter);
      }
    });
  }


  // output format

  public <K, V> void checkOutputSpecs(final FileSystem ignored) throws IOException {
    Path path = new Path("target/testData/TestIsolatedInputFormat/out");
    FileSystem fs = path.getFileSystem(conf);
    System.out.println("before checkOutputSpecs: " + fs.listStatus(path));
    callInContext(getOutputSpec(), new MapredContextualRun() {
      public void run(JobConf contextualConf) throws IOException, InterruptedException {
        @SuppressWarnings("unchecked") // wishful thinking
        OutputFormat<K, V> outputFormat = newInstanceFromSpec(contextualConf, spec, OutputFormat.class);
        outputFormat.checkOutputSpecs(ignored, contextualConf);
      }
    });
    System.out.println("after checkOutputSpecs: " + fs.listStatus(path));
  }

  public <K, V> RecordWriter<K, V> getRecordWriter(final FileSystem ignored, final String name, final Progressable p) throws IOException {
    Path path = new Path("target/testData/TestIsolatedInputFormat/out");
    FileSystem fs = path.getFileSystem(conf);
    System.out.println("before getRecordWriter: " + fs.listStatus(new Path("target/testData/TestIsolatedInputFormat/out")));
    return callInContext(getOutputSpec(), new MapredContextualCall<RecordWriter<K, V>>() {
      public RecordWriter<K, V> call(JobConf contextualConf) throws IOException, InterruptedException {
        @SuppressWarnings("unchecked") // wishful thinking
        OutputFormat<K, V> outputFormat = newInstanceFromSpec(contextualConf, spec, OutputFormat.class);
        return outputFormat.getRecordWriter(ignored, contextualConf, name, p);
      }
    });
  }

  // MapRed specific context management

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

  private static abstract class MapredContextualRun extends MapredContextualCall<Void> {

    @Override
    final public Void call(JobConf contextualConf) throws IOException, InterruptedException {
      run(contextualConf);
      return null;
    }

    abstract public void run(JobConf contextualConf) throws IOException, InterruptedException;

  }

}
