package com.twitter.isolated.hadoop.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.twitter.isolated.hadoop.ContextManager;
import com.twitter.isolated.hadoop.InputSpec;

public class MapreduceContextManager extends ContextManager {

  MapreduceContextManager(JobContext context) {
    this(context.getConfiguration());
  }

  MapreduceContextManager(Configuration conf) {
    super(conf);
  }

  private static abstract class JobContextualRun extends ContextualCall<Void> {
    private final JobContext context;

    JobContextualRun(InputSpec inputSpec, JobContext context) {
      super(inputSpec);
      this.context = context;
    }

    abstract void run(JobContext context) throws IOException, InterruptedException;

    @Override
    final public Void call(Configuration conf) throws IOException, InterruptedException {
      JobContext newContext = new JobContext(conf, context.getJobID());
      this.run(newContext);
      checkConfUpdate(conf, newContext.getConfiguration(), context.getConfiguration());
      return null;
    }
  }

  private static abstract class TaskContextualRun extends TaskContextualCall<Void> {

    TaskContextualRun(InputSpec inputSpec, TaskAttemptContext context) {
      super(inputSpec, context);
    }

    abstract void run(TaskAttemptContext context) throws IOException, InterruptedException;

    @Override
    final Void call(TaskAttemptContext context) throws IOException, InterruptedException {
      run(context);
      return null;
    }
  }

  private static abstract class TaskContextualCall<T> extends ContextualCall<T> {
    private final TaskAttemptContext context;

    TaskContextualCall(InputSpec inputSpec, TaskAttemptContext context) {
      super(inputSpec);
      this.context = context;
    }

    abstract T call(TaskAttemptContext context) throws IOException, InterruptedException;

    @Override
    final public T call(Configuration conf) throws IOException, InterruptedException {
      TaskAttemptContext newContext = new TaskAttemptContext(conf, context.getTaskAttemptID());
      T t = this.call(newContext);
      checkConfUpdate(conf, newContext.getConfiguration(), context.getConfiguration());
      return t;
    }
  }

  // methods that make sure delegated calls are executed in the right context

  List<InputSplit> getSplits(JobContext context) throws IOException {
    final List<InputSplit> finalSplits = new ArrayList<InputSplit>();
    for (final InputSpec inputSpec : getInputSpecs()) {
      callInContext(new JobContextualRun(inputSpec, context) {
        public void run(JobContext context) throws IOException, InterruptedException {
          InputFormat<?, ?> inputFormat = newInputFormat(context.getConfiguration(), inputSpec, InputFormat.class);
          List<InputSplit> splits = inputFormat.getSplits(context);
          for (InputSplit inputSplit : splits) {
            finalSplits.add(new IsolatedInputSplit(inputSpec.getId(), inputSplit, context.getConfiguration()));
          }
        }
      });
    }
    return finalSplits;
  }

  <K, V> RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
    final IsolatedInputSplit isolatedSplit = (IsolatedInputSplit)split;
    return callInContext(new TaskContextualCall<RecordReader<K, V>>(getInputSpec(isolatedSplit.getInputSpecID()), context) {
      public RecordReader<K, V> call(TaskAttemptContext context) throws IOException,
          InterruptedException {
        @SuppressWarnings("unchecked") // wishful thinking
        InputFormat<K, V> inputFormat = newInputFormat(context.getConfiguration(), inputSpec, InputFormat.class);
        return inputFormat.createRecordReader(isolatedSplit.getDelegate(), context);
      }
    });
  }

  <K, V> void initializeRecordReader(final RecordReader<K, V> delegate, InputSplit split, TaskAttemptContext context) throws IOException {
    final IsolatedInputSplit isolatedSplit = (IsolatedInputSplit)split;
    callInContext(new TaskContextualRun(getInputSpec(isolatedSplit.getInputSpecID()), context) {
      public void run(TaskAttemptContext context) throws IOException, InterruptedException {
        delegate.initialize(isolatedSplit.getDelegate(), context);
      }
    });
  }

  InputSplit deserializeSplit(final InputStream in, String inputSpecID, final String name, Configuration configuration) throws IOException {
    return callInContext(new ContextualCall<InputSplit>(getInputSpec(inputSpecID)) {
      public InputSplit call(Configuration conf) throws IOException,
          InterruptedException {
        try {
          Class<? extends InputSplit> splitClass = conf.getClassByName(name).asSubclass(InputSplit.class);
          return deserialize(in, conf, splitClass);
        } catch (ClassNotFoundException e) {
          throw new IOException("could not resolve class " + name, e);
        }
      }

      // we need to define a common T for these calls to work together
      private <T extends InputSplit> InputSplit deserialize(final InputStream in, Configuration conf, Class<T> splitClass) throws IOException {
        T delegateInstance = ReflectionUtils.newInstance(splitClass, conf);
        SerializationFactory factory = new SerializationFactory(conf);
        Deserializer<T> deserializer = factory.getDeserializer(splitClass);
        deserializer.open(in);
        return deserializer.deserialize(delegateInstance);
      }
    });
  }
}
