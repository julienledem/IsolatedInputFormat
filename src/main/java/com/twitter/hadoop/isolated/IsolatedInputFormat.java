package com.twitter.hadoop.isolated;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class IsolatedInputFormat<K, V> extends InputFormat<K, V> {

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    final RecordReader<K, V> delegate = IsolatedInputFormat.<K,V>resolverFromConf(context).createRecordReader((IsolatedInputSplit)split, context);
    return new RecordReader<K, V>() {

      @Override
      public void close() throws IOException {
        delegate.close();

      }

      @Override
      public K getCurrentKey() throws IOException, InterruptedException {
        return delegate.getCurrentKey();
      }

      @Override
      public V getCurrentValue() throws IOException, InterruptedException {
        return delegate.getCurrentValue();
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return delegate.getProgress();
      }

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context)
          throws IOException, InterruptedException {
        IsolatedInputFormat.<K,V>resolverFromConf(context).initializeRecordReader(delegate, (IsolatedInputSplit)split, context);
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        return delegate.nextKeyValue();
      }
    };
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return resolverFromConf(context).getSplits(inputSpecsFromConf(context.getConfiguration()), context);
  }

  static String key(String... values) {
    StringBuilder sb = new StringBuilder("com.twitter.isolated");
    for (String value : values) {
      sb.append(".").append(value);
    }
    return sb.toString();
  }

  private static Collection<String> getEntries(Configuration conf, String key) {
    Set<String> result = new TreeSet<String>();
    for (String property : getEntriesFull(conf, key)) {
      int nextDot = property.indexOf('.');
      if (nextDot != -1) {
        result.add(property.substring(0, nextDot));
      }
    }
    return result;
  }

  private static Collection<String> getEntriesFull(Configuration conf, String key) {
    Set<String> result = new TreeSet<String>();
    String prefix = key + ".";
    for (Entry<String, String> e : conf) {
      String property = e.getKey();
      if (property.startsWith(prefix)) {
        result.add(property.substring(prefix.length()));
      }
    }
    return result;
  }

  static <K, V> InputFormatResolver<K, V> resolverFromConf(JobContext context) {
    return resolverFromConf(context.getConfiguration());
  }

  static <K, V> InputFormatResolver<K, V> resolverFromConf(Configuration conf) {
    return new InputFormatResolver<K, V>(librariesFromConf(conf), inputFormatDefinitionsFromConf(conf));
  }

  static List<InputSpec> inputSpecsFromConf(Configuration conf) {
    List<InputSpec> result = new ArrayList<InputSpec>();
    for (String spec : getEntries(conf, key("inputspec"))) {
      String ifName = conf.get(key("inputspec", spec, "inputformat"));
      Map<String, String> specConf = getConf(conf, key("inputspec", spec));
      result.add(new InputSpec(spec, ifName, specConf));
    }
    return result;
  }

  static List<InputFormatDefinition> inputFormatDefinitionsFromConf(Configuration conf) {
    List<InputFormatDefinition> result = new ArrayList<InputFormatDefinition>();
    for (String inputformat : getEntries(conf, key("inputformat"))) {
      String className = conf.get(key("inputformat", inputformat, "class"));
      String lib = conf.get(key("inputformat", inputformat, "library"));
      Map<String, String> ifConf = getConf(conf, key("inputformat", inputformat));
      result.add(new InputFormatDefinition(inputformat, lib, className, ifConf));
    }
    return result;
  }

  static List<Library> librariesFromConf(Configuration conf) {
    if (conf == null) {
      throw new NullPointerException("conf");
    }
    List<Library> result = new ArrayList<Library>();
    for (String lib : getEntries(conf, key("library"))) {
      String[] paths = conf.get(key("library", lib, "paths"), "").split(" ");
      List<Path> jars = new ArrayList<Path>();
      for (String p : paths) {
        if (p.length() > 0) {
          jars.add(new Path(p));
        }
      }
      result.add(new Library(lib, jars));
    }
    return result;
  }

  public static void setLibraries(Job job, Collection<Library> libraries) {
    Configuration conf = job.getConfiguration();
    for (Library library : libraries) {
      List<Path> jars = library.getJars();
      String urlsString = "";
      for (Path p : jars) {
        urlsString += p.toString() + " ";
      }
      conf.set(key("library", library.getName(), "paths"), urlsString);
    }
  }

  public static void setInputFormats(Job job, Collection<InputFormatDefinition> inputFormats) {
    Configuration conf = job.getConfiguration();
    for (InputFormatDefinition ifDef : inputFormats) {
      conf.set(key("inputformat", ifDef.getName(), "class"), ifDef.getInputFormatClassName());
      if (ifDef.getLibraryName() != null) {
        conf.set(key("inputformat", ifDef.getName(), "library"), ifDef.getLibraryName());
      }
      setConf(conf, key("inputformat", ifDef.getName()), ifDef.getConf());
    }
  }

  private static void setConf(Configuration conf, String key, Map<String, String> m) {
    for (Entry<String, String> e: m.entrySet()) {
      conf.set(key + ".conf." + e.getKey(), e.getValue());
    }
  }

  private static Map<String, String> getConf(Configuration conf, String baseKey) {
    Map<String, String> result = new TreeMap<String, String>();
    for (String key : getEntriesFull(conf, baseKey + ".conf")) {
      result.put(key, conf.get(baseKey + ".conf." + key));
    }
    return result;
  }

  public static void setInputSpecs(Job job, Collection<InputSpec> inputSpecs) {
    setInputSpecs(job.getConfiguration(), inputSpecs);
  }

  public static void setInputSpecs(Configuration conf, Collection<InputSpec> inputSpecs) {
    for (InputSpec inputSpec : inputSpecs) {
      conf.set(key("inputspec", inputSpec.getId(), "inputformat"), inputSpec.getInputFormatName());
      setConf(conf, key("inputspec", inputSpec.getId()), inputSpec.getConf());
    }
  }

}
