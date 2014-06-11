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

  /**
   * saves the conf in m in the provided conf by prefixing all the keys with the provided key.
   * @param conf where to save
   * @param key key prefix for the conf
   * @param m the conf to save.
   */
  static void setConf(Configuration conf, String key, Map<String, String> m) {
    for (Entry<String, String> e: m.entrySet()) {
      conf.set(key + ".conf." + e.getKey(), e.getValue());
    }
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

  static List<Spec> inputSpecsFromConf(Configuration conf) {
    List<Spec> result = new ArrayList<Spec>();
    for (String spec : getEntries(conf, key("inputspec"))) {
      String ifName = conf.get(key("inputspec", spec, "inputformat"));
      Map<String, String> specConf = getConf(conf, key("inputspec", spec));
      result.add(new Spec(spec, ifName, specConf));
    }
    return result;
  }

  static Spec outputSpecFromConf(Configuration conf) {
    String ofName = conf.get(key("outputspec", "outputformat"));
    Map<String, String> specConf = getConf(conf, key("outputspec"));
    return new Spec("output", ofName, specConf);
  }

  static List<ClassDefinition> classDefinitionsFromConf(Configuration conf) {
    List<ClassDefinition> result = new ArrayList<ClassDefinition>();
    for (String inputformat : getEntries(conf, key("class"))) {
      String className = conf.get(key("class", inputformat, "name"));
      String lib = conf.get(key("class", inputformat, "library"));
      Map<String, String> ifConf = getConf(conf, key("class", inputformat));
      result.add(new ClassDefinition(inputformat, lib, className, ifConf));
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
      if (paths.length == 0) {
        throw new IllegalArgumentException("the library " + lib + " has not jars defined");
      }
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
      StringBuilder urlsString = new StringBuilder();
      for (Path p : jars) {
        urlsString.append(p.toString()).append(" ");
      }
      conf.set(key("library", library.getName(), "paths"), urlsString.toString());
    }
  }

  public static void setClassDefinitions(Configuration conf, Collection<ClassDefinition> inputFormats) {
    for (ClassDefinition ifDef : inputFormats) {
      conf.set(key("class", ifDef.getName(), "name"), ifDef.getClassName());
      if (ifDef.getLibraryName() != null) {
        conf.set(key("class", ifDef.getName(), "library"), ifDef.getLibraryName());
      }
      setConf(conf, key("class", ifDef.getName()), ifDef.getConf());
    }
  }

  public static void setInputSpecs(Configuration conf, Collection<Spec> inputSpecs) {
    for (Spec inputSpec : inputSpecs) {
      conf.set(key("inputspec", inputSpec.getId(), "inputformat"), inputSpec.getClassDefinition());
      setConf(conf, key("inputspec", inputSpec.getId()), inputSpec.getConf());
    }
  }

  public static void setOutputSpec(Configuration conf, Spec outputSpec) {
    conf.set(key("outputspec", "outputformat"), outputSpec.getClassDefinition());
    setConf(conf, key("outputspec"), outputSpec.getConf());
  }

  private static Map<String, String> getConf(Configuration conf, String baseKey) {
    Map<String, String> result = new TreeMap<String, String>();
    for (String key : getEntriesFull(conf, baseKey + ".conf")) {
      result.put(key, conf.get(baseKey + ".conf." + key));
    }
    return result;
  }

}
