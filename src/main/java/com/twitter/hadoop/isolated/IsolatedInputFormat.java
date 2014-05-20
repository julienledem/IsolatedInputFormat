package com.twitter.hadoop.isolated;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configurable;
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
        delegate.initialize(((IsolatedInputSplit)split).getDelegate(), context);
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

  static <K, V> InputFormatResolver<K, V> resolverFromConf(JobContext context) {
    return resolverFromConf(context.getConfiguration());
  }

  static <K, V> InputFormatResolver<K, V> resolverFromConf(Configuration conf) {
    return new InputFormatResolver<K, V>(librariesFromConf(conf), inputFormatDefinitionsFromConf(conf));
  }

  private List<InputSpec> inputSpecsFromConf(Configuration conf) {
    List<InputSpec> result = new ArrayList<InputSpec>();
    int count = Integer.parseInt(conf.get(key("inputspecs")));
    for (int i = 0; i< count; ++i) {
      String id = String.valueOf(i);
      String ifName = conf.get(key("inputspec", id, "inputformat"));
      Map<String, String> specConf = getConf(conf, key("inputspec", id));
      result.add(new InputSpec(ifName, specConf));
    }
    return result;
  }

  static List<InputFormatDefinition> inputFormatDefinitionsFromConf(Configuration conf) {
    List<InputFormatDefinition> result = new ArrayList<InputFormatDefinition>();
    String[] ifs = conf.get(key("inputformats")).split(" ");
    for (String inputformat : ifs) {
      if (inputformat.length() > 0) {
        String className = conf.get(key("inputformat", inputformat, "class"));
        String lib = conf.get(key("inputformat", inputformat, "library"));
        Map<String, String> ifConf = getConf(conf, key("inputformat", inputformat));
        result.add(new InputFormatDefinition(inputformat, lib, className, ifConf));
      }
    }
    return result;
  }

  static List<Library> librariesFromConf(Configuration conf) {
    if (conf == null) {
      throw new NullPointerException("conf");
    }
    List<Library> result = new ArrayList<Library>();
    String[] libs = conf.get(key("libraries")).split(" ");
    for (String lib : libs) {
      if (lib.length() > 0) {
        String[] paths = conf.get(key("library", lib, "urls"), "").split(" ");
        List<Path> jars = new ArrayList<Path>();
        for (String p : paths) {
          if (p.length() > 0) {
              jars.add(new Path(p));
          }
        }
        result.add(new Library(lib, jars));
      }
    }
    return result;
  }

  private static String key(String... values) {
    StringBuilder sb = new StringBuilder("com.twitter.isolated");
    for (String value : values) {
      sb.append(".").append(value);
    }
    return sb.toString();
  }

  public static void setLibraries(Job job, Collection<Library> libraries) {
    Configuration conf = job.getConfiguration();
    String librariesString = "";
    for (Library library : libraries) {
      librariesString += library.getName() + " ";
      List<Path> jars = library.getJars();
      String urlsString = "";
      for (Path p : jars) {
        urlsString += p.toString() + " ";
      }
      conf.set(key("library", library.getName(), "urls"), urlsString);
    }
    conf.set(key("libraries"), librariesString);
  }

  public static void setInputFormats(Job job, Collection<InputFormatDefinition> inputFormats) {
    Configuration conf = job.getConfiguration();
    String ifsString = "";
    for (InputFormatDefinition ifDef : inputFormats) {
      ifsString += ifDef.getName() + " ";
      conf.set(key("inputformat", ifDef.getName(), "class"), ifDef.getInputFormatClassName());
      conf.set(key("inputformat", ifDef.getName(), "library"), ifDef.getLibraryName());
      setConf(conf, key("inputformat", ifDef.getName()), ifDef.getConf());
    }
    conf.set(key("inputformats"), ifsString);
  }

  private static void setConf(Configuration conf, String key, Map<String, String> m) {
    String keys = "";
    for (Entry<String, String> e: m.entrySet()) {
      conf.set(key + ".conf." + e.getKey(), e.getValue());
      keys += " " + e.getKey();
    }
    conf.set(key + ".keys", keys);
  }

  private static Map<String, String> getConf(Configuration conf, String baseKey) {
    Map<String, String> result = new HashMap<String, String>();
    String[] keys = conf.get(baseKey + ".keys", "").split(" ");
    for (String key : keys) {
      if (key.length() > 0) {
        result.put(key, conf.get(baseKey + ".conf." + key));
      }
    }
    return result;
  }

  public static void setInputSpecs(Job job, Collection<InputSpec> inputSpecs) {
    Configuration conf = job.getConfiguration();
    int i = 0;
    for (InputSpec inputSpec : inputSpecs) {
      String inputSpecId = String.valueOf(i);
      conf.set(key("inputspec", inputSpecId, "inputformat"), inputSpec.getInputFormatName());
      setConf(conf, key("inputspec", inputSpecId), inputSpec.getConf());
      ++i;
    }
    conf.set(key("inputspecs"), String.valueOf(i));
  }

}
