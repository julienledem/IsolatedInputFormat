package com.twitter.isolated.hadoop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class IsolatedConf {
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

  public static void setLibraries(Configuration conf, Collection<Library> libraries) {
    for (Library library : libraries) {
      List<Path> jars = library.getJars();
      String urlsString = "";
      for (Path p : jars) {
        urlsString += p.toString() + " ";
      }
      conf.set(key("library", library.getName(), "paths"), urlsString);
    }
  }

  public static void setInputFormats(Configuration conf, Collection<InputFormatDefinition> inputFormats) {
    for (InputFormatDefinition ifDef : inputFormats) {
      conf.set(key("inputformat", ifDef.getName(), "class"), ifDef.getInputFormatClassName());
      if (ifDef.getLibraryName() != null) {
        conf.set(key("inputformat", ifDef.getName(), "library"), ifDef.getLibraryName());
      }
      ContextManager.setConf(conf, key("inputformat", ifDef.getName()), ifDef.getConf());
    }
  }

  public static void setInputSpecs(Configuration conf, Collection<InputSpec> inputSpecs) {
    ContextManager.setInputSpecs(conf, inputSpecs);
  }

  private static Map<String, String> getConf(Configuration conf, String baseKey) {
    Map<String, String> result = new TreeMap<String, String>();
    for (String key : getEntriesFull(conf, baseKey + ".conf")) {
      result.put(key, conf.get(baseKey + ".conf." + key));
    }
    return result;
  }

}
