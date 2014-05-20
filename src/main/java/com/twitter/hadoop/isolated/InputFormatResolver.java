package com.twitter.hadoop.isolated;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;


class InputFormatResolver<K, V> {

  private static URL[] toURLs(List<Path> jars) {
    try {
      URL[] result = new URL[jars.size()];
      for (int i = 0; i < result.length; i++) {
        Path path = jars.get(i);
        Configuration conf = new Configuration();
        FileSystem fs = path.getFileSystem(conf);
        validate(path, fs);
        URI uri = path.toUri();
        if (!uri.getScheme().equals("file")) {
          File tmp = File.createTempFile(path.getName(), ".jar");
          tmp.deleteOnExit();
          FSDataInputStream s = fs.open(path);
          FileOutputStream fso = new FileOutputStream(tmp);
          try {
            IOUtils.copyBytes(s, fso, conf);
          } finally {
            IOUtils.closeStream(fso);
            IOUtils.closeStream(s);
          }
          result[i] = tmp.toURI().toURL();
        } else {
          throw new RuntimeException("jars should be on HDFS: " + path);
//          result[i] = uri.toURL();
        }
      }
      return result;
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void validate(Path path, FileSystem fs) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(path);
    if (fileStatus.isDir()) {
      throw new RuntimeException(path + " should be a jar");
    }
    byte[] header = new byte[4];
    FSDataInputStream s = fs.open(path);
    s.readFully(header);
    s.close();
    byte[] expected = { 80, 75, 3, 4 }; // zip file header
    if (!Arrays.equals(expected, header)) {
      throw new RuntimeException("path " + path + " is not a valid jar");
    }
  }

  private static final class IsolatedClassLoader extends URLClassLoader {
    private IsolatedClassLoader(Library lib, ClassLoader parent) {
      super(toURLs(lib.getJars()), parent);
    }

    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve)
        throws ClassNotFoundException {
      Class<?> c = findLoadedClass(name);
      if (c == null && !apiClass(name)) {
        try {
          c = findClass(name);
        } catch (ClassNotFoundException e) { }
      }
      if (c == null) { // try parent
        c = getParent().loadClass(name);
      }
      if (resolve) {
        resolveClass(c);
      }
      return c;
    }

    private boolean apiClass(String name) {
      return false;
    }
  }

  private static Map<Library, ClassLoader> classLoaderByLib = new HashMap<Library, ClassLoader>();

  /**
   * ensures we always return the same class loader for the same library definition
   * otherwise you can get "Foo can not be cast to Foo" errors
   * @param lib the lib definition
   * @return the corresponding classloader
   */
  private static ClassLoader getClassLoader(Library lib) {
    ClassLoader classLoader = classLoaderByLib.get(lib);
    if (classLoader == null) {
      ClassLoader parent = InputFormatResolver.class.getClassLoader();
      classLoader = lib == null ? parent : new IsolatedClassLoader(lib, parent);
      classLoaderByLib.put(lib, classLoader);
    }
    return classLoader;
  }

  static Configuration newConf(Configuration conf, InputSpec inputSpec, InputFormatDefinition inputFormatDefinition) {
    Configuration newConf = new Configuration(conf);
    applyConf(inputFormatDefinition.getConf(), newConf);
    applyConf(inputSpec.getConf(), newConf);
    return newConf;
  }

  static void applyConf(Map<String, String> map, Configuration conf) {
    for (Entry<String, String> entry : map.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
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
  private Map<String, Class<InputFormat<K, V>>> inputFormatByName = new HashMap<String, Class<InputFormat<K, V>>>();
  private Map<String, ClassLoader> classLoaderByInputFormatName = new HashMap<String, ClassLoader>();

  InputFormatResolver(List<Library> libraries, List<InputFormatDefinition> inputFormatDefinitions) {
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

  private static interface ContextualCall<T> {
    T call(Configuration conf) throws IOException, InterruptedException;
  }

  private static interface ContextualRun {
    void run(Configuration conf) throws IOException, InterruptedException;
  }

  private void runInContext(Configuration conf, InputSpec inputSpec, final ContextualRun runable) throws IOException {
    callInContext(conf, inputSpec, new ContextualCall<Void>() {
      @Override
      public Void call(Configuration conf) throws IOException, InterruptedException {
        runable.run(conf);
        return null;
      }
    });
  }

  private <T> T callInContext(Configuration conf, InputSpec inputSpec, ContextualCall<T> callable) throws IOException {
    Thread currentThread = Thread.currentThread();
    ClassLoader contextClassLoader = currentThread.getContextClassLoader();
    try {
      InputFormatDefinition inputFormatDefinition = lookup(inputFormatDefByName, inputSpec.getInputFormatName());
      currentThread.setContextClassLoader(lookup(classLoaderByInputFormatName, inputSpec.getInputFormatName()));
      Configuration newConf = newConf(conf, inputSpec, inputFormatDefinition);
      return callable.call(newConf);
    } catch (InterruptedException e) {
      throw new IOException("thread interrupted", e);
    } finally {
      currentThread.setContextClassLoader(contextClassLoader);
    }
  }

  List<InputSplit> getSplits(List<InputSpec> inputSpecs, final JobContext context)
      throws IOException, InterruptedException {
    final List<InputSplit> finalSplits = new ArrayList<InputSplit>();
    for (final InputSpec inputSpec : inputSpecs) {
      runInContext(context.getConfiguration(), inputSpec, new ContextualRun() {
        public void run(Configuration conf) throws IOException, InterruptedException {
          InputFormat<K, V> inputFormat = newInputFormat(inputSpec.getInputFormatName());
          List<InputSplit> splits = inputFormat.getSplits(new JobContext(conf, context.getJobID()));
          for (InputSplit inputSplit : splits) {
            finalSplits.add(new IsolatedInputSplit(inputSpec, inputSplit, conf));
          }
        }
      });
    }
    return finalSplits;
  }

  RecordReader<K, V> createRecordReader(final IsolatedInputSplit split, final TaskAttemptContext context)
      throws IOException, InterruptedException {
    return callInContext(context.getConfiguration(), split.getInputSpec(), new ContextualCall<RecordReader<K, V>>() {
      public RecordReader<K, V> call(Configuration conf) throws IOException,
          InterruptedException {
        InputFormat<K, V> inputFormat = newInputFormat(split.getInputSpec().getInputFormatName());
        return inputFormat.createRecordReader(split.getDelegate(), new TaskAttemptContext(conf, context.getTaskAttemptID()));
      }
    });
  }

  <T extends InputSplit> T deserializeSplit(final InputStream in, InputSpec inputSpec, final String name, Configuration configuration) throws IOException {
    return callInContext(configuration, inputSpec, new ContextualCall<T>() {
      public T call(Configuration conf) throws IOException,
          InterruptedException {
        try {
          Class<T> splitClass = (Class<T>)conf.getClassByName(name).asSubclass(InputSplit.class);
          T delegateInstance = ReflectionUtils.newInstance(splitClass, conf);
          SerializationFactory factory = new SerializationFactory(conf);
          Deserializer<T> deserializer = factory.getDeserializer(splitClass);
          deserializer.open(in);
          return deserializer.deserialize(delegateInstance);
        } catch (ClassNotFoundException e) {
          throw new IOException("could not resolve class " + name, e);
        }
      }
    });
  }
}