package com.twitter.isolated.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

class MapreduceTaskAttemptContextManager extends MapreduceContextManager {

  private final TaskAttemptContext globalTaskContext;

  MapreduceTaskAttemptContextManager(TaskAttemptContext globalTaskContext) {
    super(globalTaskContext);
    this.globalTaskContext = globalTaskContext;
  }

  <K, V> RecordReader<K, V> createRecordReader(InputSplit split) throws IOException {
    final IsolatedInputSplit isolatedSplit = (IsolatedInputSplit)split;
    return callInContext(isolatedSplit.getInputSpecID(), new TaskContextualCall<RecordReader<K, V>>() {
      public RecordReader<K, V> call(TaskCallContext ctxt) throws IOException,
          InterruptedException {
        @SuppressWarnings("unchecked") // wishful thinking
        InputFormat<K, V> inputFormat = ctxt.newInstanceFromSpec(InputFormat.class);
        return inputFormat.createRecordReader(isolatedSplit.getDelegate(), ctxt.localTaskContext);
      }
    });
  }

  <K, V> void initializeRecordReader(final RecordReader<K, V> delegate, InputSplit split) throws IOException {
    final IsolatedInputSplit isolatedSplit = (IsolatedInputSplit)split;
    callInContext(isolatedSplit.getInputSpecID(), new TaskContextualRun() {
      public void run(TaskCallContext ctxt) throws IOException, InterruptedException {
        delegate.initialize(isolatedSplit.getDelegate(), ctxt.localTaskContext);
      }
    });
  }

  OutputCommitter getOutputCommitter() throws IOException {
    return callInContext(getOutputSpec(), new TaskContextualCall<OutputCommitter>() {
      @Override
      OutputCommitter call(TaskCallContext ctxt) throws IOException, InterruptedException {
        OutputFormat<?, ?> outputFormat = ctxt.newInstanceFromSpec(OutputFormat.class);
        return outputFormat.getOutputCommitter(ctxt.localTaskContext);
      }
    });
  }

  <K,V> RecordWriter<K, V> getRecordWriter() throws IOException {
    return callInContext(getOutputSpec(), new TaskContextualCall<RecordWriter<K, V>>() {
      @Override
      RecordWriter<K, V> call(TaskCallContext ctxt) throws IOException, InterruptedException {
        @SuppressWarnings("unchecked") // wishful thinking
        OutputFormat<K, V> outputFormat = ctxt.newInstanceFromSpec(OutputFormat.class);
        return outputFormat.getRecordWriter(ctxt.localTaskContext);
      }
    });
  }

  private static class TaskCallContext extends CallContext {

    private final TaskAttemptContext localTaskContext;

    public TaskCallContext(CallContext ctxt, TaskAttemptContext localTaskContext) {
      super(ctxt, localTaskContext.getConfiguration());
      this.localTaskContext = localTaskContext;
    }

  }

  private abstract class TaskContextualRun extends TaskContextualCall<Void> {

    abstract void run(TaskCallContext context) throws IOException, InterruptedException;

    @Override
    final Void call(TaskCallContext context) throws IOException, InterruptedException {
      run(context);
      return null;
    }
  }

  private abstract class TaskContextualCall<T> extends ContextualCall<T> {

    abstract T call(TaskCallContext context) throws IOException, InterruptedException;

    @Override
    final public T call(CallContext ctxt) throws IOException, InterruptedException {
      TaskAttemptContext newContext = new TaskAttemptContext(ctxt.localConf(), globalTaskContext.getTaskAttemptID());
      return this.call(new TaskCallContext(ctxt, newContext));
    }
  }
}
