package com.twitter.hadoop.isolated;

import java.io.DataInput;
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

  private final class IsolatedClassLoader extends URLClassLoader {
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

  private static Map<String, ClassLoader> classLoaderByLibName = new HashMap<String, ClassLoader>();
  private Map<String, Class<InputFormat<K, V>>> inputFormatByName = new HashMap<String, Class<InputFormat<K, V>>>();
  private Map<String, InputFormatDefinition> inputFormatDefByName = new HashMap<String, InputFormatDefinition>();

  InputFormatResolver(List<Library> libraries, List<InputFormatDefinition> inputFormatDefinitions) {
    for (Library library : libraries) {
      if (!classLoaderByLibName.containsKey(library.getName())) {
        classLoaderByLibName.put(library.getName(), createClassLoader(library));
      }
    }
    for (InputFormatDefinition inputFormatDef : inputFormatDefinitions) {
      inputFormatDefByName.put(inputFormatDef.getName(), inputFormatDef);
      try {
        @SuppressWarnings("unchecked")
        Class<InputFormat<K, V>> inputFormatClass = (Class<InputFormat<K, V>>)
          classLoaderByLibName.get(inputFormatDef.getLibraryName())
            .loadClass(inputFormatDef.getInputFormatClassName())
            .asSubclass(InputFormat.class);
        inputFormatByName.put(inputFormatDef.getName(), inputFormatClass);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("InputFormat not found " + inputFormatDef.getInputFormatClassName() + " in lib " + inputFormatDef.getLibraryName(), e);
      }
    }
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

  private InputFormat<K, V> newInputFormat(String inputFormatName) {
    try {
      return inputFormatByName.get(inputFormatName).newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private ClassLoader createClassLoader(Library lib) {
    return new IsolatedClassLoader(lib, this.getClass().getClassLoader());
  }

  List<InputSplit> getSplits(List<InputSpec> inputSpecs, JobContext context)
      throws IOException, InterruptedException {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      List<InputSplit> finalSplits = new ArrayList<InputSplit>();
      for (InputSpec inputSpec : inputSpecs) {
        InputFormatDefinition inputFormatDefinition = inputFormatDefByName.get(inputSpec.getInputFormatName());
        InputFormat<K, V> inputFormat = newInputFormat(inputSpec.getInputFormatName());
        Thread.currentThread().setContextClassLoader(inputFormat.getClass().getClassLoader());
        Configuration newConf = newConf(context.getConfiguration(), inputSpec, inputFormatDefinition);
        List<InputSplit> splits = inputFormat.getSplits(new JobContext(newConf, context.getJobID()));
        for (InputSplit inputSplit : splits) {
          finalSplits.add(new IsolatedInputSplit(inputSpec, inputSplit, newConf));
        }
      }
      return finalSplits;
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
  }

  RecordReader<K, V> createRecordReader(IsolatedInputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      InputFormat<K, V> inputFormat = newInputFormat(split.getInputSpec().getInputFormatName());
      Thread.currentThread().setContextClassLoader(inputFormat.getClass().getClassLoader());
      Configuration newConf = newConf(context.getConfiguration(), split.getInputSpec(), inputFormatDefByName.get(split.getInputSpec().getInputFormatName()));
      return inputFormat.createRecordReader(split.getDelegate(), new TaskAttemptContext(newConf, context.getTaskAttemptID()));
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
  }

  <T extends InputSplit> T deserializeSplit(InputStream in, InputSpec inputSpec, String name, Configuration configuration) throws IOException {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(classLoaderByLibName.get(inputFormatDefByName.get(inputSpec.getInputFormatName()).getLibraryName()));
      Configuration newConf = newConf(configuration, inputSpec, inputFormatDefByName.get(inputSpec.getInputFormatName()));
      Class<T> splitClass = (Class<T>)newConf.getClassByName(name).asSubclass(InputSplit.class);
      T delegateInstance = ReflectionUtils.newInstance(splitClass, newConf);
      SerializationFactory factory = new SerializationFactory(newConf);
      Deserializer<T> deserializer = factory.getDeserializer(splitClass);
      deserializer.open(in);
      return deserializer.deserialize(delegateInstance);
    } catch (ClassNotFoundException e) {
      throw new IOException("could not resolve class " + name, e);
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
  }
}