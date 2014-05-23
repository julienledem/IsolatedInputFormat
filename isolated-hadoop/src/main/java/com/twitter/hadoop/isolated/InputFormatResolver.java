package com.twitter.hadoop.isolated;

import static com.twitter.hadoop.isolated.LibraryManager.getClassLoader;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;


class InputFormatResolver<K, V> {


  private static void applyConf(Configuration conf, Map<String, String> map) {
    for (Entry<String, String> entry : map.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
  }

  static Configuration newConf(final Configuration conf, final InputSpec inputSpec, final InputFormatDefinition inputFormatDefinition) {
    Configuration newConf = new Configuration(conf);
    applyConf(newConf, inputFormatDefinition.getConf());
    applyConf(newConf, inputSpec.getConf());
    return newConf;
  }

  private static <T> T lookup(Map<String, T> map, String key) {
    T lookedUp = map.get(key);
    if (lookedUp == null) {
      throw new IllegalArgumentException(key + " not found in " + map.keySet());
    }
    return lookedUp;
  }

  private Map<String, Library> libByName = new HashMap<String, Library>();
  private Map<String, InputFormatDefinition> inputFormatDefByName = new HashMap<String, InputFormatDefinition>();
  private Map<String, InputSpec> inputSpecByName = new HashMap<String, InputSpec>();
  private Map<String, Class<InputFormat<K, V>>> inputFormatByName = new HashMap<String, Class<InputFormat<K, V>>>();
  private Map<String, ClassLoader> classLoaderByInputFormatName = new HashMap<String, ClassLoader>();

  InputFormatResolver(List<Library> libraries, List<InputFormatDefinition> inputFormatDefinitions, List<InputSpec> inputSpecs) {
    for (Library library : libraries) {
      libByName.put(library.getName(), library);
    }
    for (InputFormatDefinition inputFormatDef : inputFormatDefinitions) {
      inputFormatDefByName.put(inputFormatDef.getName(), inputFormatDef);
      Library library = inputFormatDef.getLibraryName() == null ? null : lookup(libByName, inputFormatDef.getLibraryName());
      ClassLoader classLoader = getClassLoader(library);
      classLoaderByInputFormatName.put(inputFormatDef.getName(), classLoader);
      try {
        @SuppressWarnings("unchecked")
        Class<InputFormat<K, V>> inputFormatClass = (Class<InputFormat<K, V>>)
          classLoader
            .loadClass(inputFormatDef.getInputFormatClassName())
            .asSubclass(InputFormat.class);
        inputFormatByName.put(inputFormatDef.getName(), inputFormatClass);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("InputFormat not found " + inputFormatDef.getInputFormatClassName() + " in lib " + inputFormatDef.getLibraryName(), e);
      }
    }
    for (InputSpec spec : inputSpecs) {
      lookup(inputFormatDefByName, spec.getInputFormatName()); // validate conf
      inputSpecByName.put(spec.getId(), spec);
    }
  }

  private InputFormat<K, V> newInputFormat(String inputFormatName) {
    try {
      return lookup(inputFormatByName, inputFormatName).newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static abstract class JobContextualRun extends ContextualCall<Void> {
    private final JobContext context;

    JobContextualRun(InputSpec inputSpec, JobContext context) {
      super(inputSpec, context.getConfiguration());
      this.context = context;
    }

    abstract void run(JobContext context) throws IOException, InterruptedException;

    @Override
    final public Void call(Configuration conf) throws IOException, InterruptedException {
      JobContext newContext = new JobContext(conf, context.getJobID());
      this.run(newContext);
      checkConfUpdate(conf, newContext.getConfiguration());
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
      super(inputSpec, context.getConfiguration());
      this.context = context;
    }

    abstract T call(TaskAttemptContext context) throws IOException, InterruptedException;

    @Override
    final public T call(Configuration conf) throws IOException, InterruptedException {
      TaskAttemptContext newContext = new TaskAttemptContext(conf, context.getTaskAttemptID());
      T t = this.call(newContext);
      checkConfUpdate(conf, newContext.getConfiguration());
      return t;
    }
  }

  private static abstract class ContextualCall<T> {
    private final Configuration conf;
    protected final InputSpec inputSpec;

    ContextualCall(InputSpec inputSpec, Configuration conf) {
      this.inputSpec = inputSpec;
      this.conf = conf;
    }

    abstract T call(Configuration conf) throws IOException, InterruptedException;

    final void checkConfUpdate(Configuration before, Configuration after) {
      for (Entry<String, String> e : after) {
        String previous = before.getRaw(e.getKey());
        if (previous == null || !previous.equals(e.getValue())) {
          inputSpec.getConf().put(e.getKey(), e.getValue());
        }
        IsolatedInputFormat.setInputSpecs(conf, asList(inputSpec));
      }
    }

  }

  /**
   * guarantees that the delegated calls are done in the right context:
   *  - classloader to the proper lib
   *  - configuration from the proper inputSpec and InputFormat
   *  - configuration modifications are propagated in isolation
   * @param callable what to do
   * @return what callable returns
   * @throws IOException
   */
  private <T> T callInContext(ContextualCall<T> callable) throws IOException {
    Thread currentThread = Thread.currentThread();
    ClassLoader contextClassLoader = currentThread.getContextClassLoader();
    try {
      InputSpec inputSpec = callable.inputSpec;
      InputFormatDefinition inputFormatDefinition = lookup(inputFormatDefByName, inputSpec.getInputFormatName());
      currentThread.setContextClassLoader(lookup(classLoaderByInputFormatName, inputSpec.getInputFormatName()));
      Configuration newConf = newConf(callable.conf, inputSpec, inputFormatDefinition);
      return callable.call(newConf);
    } catch (InterruptedException e) {
      throw new IOException("thread interrupted", e);
    } finally {
      currentThread.setContextClassLoader(contextClassLoader);
    }
  }

  // method that make sure delegated calls are executed in the right context

  List<InputSplit> getSplits(List<InputSpec> inputSpecs, JobContext context)
      throws IOException, InterruptedException {
    final List<InputSplit> finalSplits = new ArrayList<InputSplit>();
    for (final InputSpec inputSpec : inputSpecs) {
      callInContext(new JobContextualRun(inputSpec, context) {
        public void run(JobContext context) throws IOException, InterruptedException {
          InputFormat<K, V> inputFormat = newInputFormat(inputSpec.getInputFormatName());
          List<InputSplit> splits = inputFormat.getSplits(context);
          for (InputSplit inputSplit : splits) {
            finalSplits.add(new IsolatedInputSplit(inputSpec.getId(), inputSplit, context.getConfiguration()));
          }
        }
      });
    }
    return finalSplits;
  }

  RecordReader<K, V> createRecordReader(final IsolatedInputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return callInContext(new TaskContextualCall<RecordReader<K, V>>(lookup(inputSpecByName, split.getInputSpecID()), context) {
      public RecordReader<K, V> call(TaskAttemptContext context) throws IOException,
          InterruptedException {
        InputFormat<K, V> inputFormat = newInputFormat(inputSpec.getInputFormatName());
        return inputFormat.createRecordReader(split.getDelegate(), context);
      }
    });
  }

  InputSplit deserializeSplit(final InputStream in, String inputSpecID, final String name, Configuration configuration) throws IOException {
    return callInContext(new ContextualCall<InputSplit>(lookup(inputSpecByName, inputSpecID), configuration) {
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

  void initializeRecordReader(final RecordReader<K, V> delegate, final IsolatedInputSplit split, TaskAttemptContext context) throws IOException {
    callInContext(new TaskContextualRun(lookup(inputSpecByName, split.getInputSpecID()), context) {
      public void run(TaskAttemptContext context) throws IOException, InterruptedException {
        delegate.initialize(split.getDelegate(), context);
      }
    });
  }
}