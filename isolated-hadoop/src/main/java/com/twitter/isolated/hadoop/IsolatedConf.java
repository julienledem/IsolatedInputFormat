package com.twitter.isolated.hadoop;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

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

public class IsolatedConf {

  private static final String KEY_PREFIX = "com.twitter.isolated";
  private static final String PATHS = "paths";
  private static final String NAME = "name";
  private static final String LIBRARY = "library";
  private static final String CLASS = "class";
  private static final String CONF = "conf";
  private static final String SPEC = "spec";
  private static final String INPUTSPECS = "inputspecs";
  private static final String OUTPUTSPEC = "outputspec";

  /**
   * saves the conf in m in the provided conf by prefixing all the keys with the provided key.
   * @param conf where to save
   * @param key key prefix for the conf
   * @param m the conf to save.
   */
  static void setConf(Configuration conf, String key, Map<String, String> m) {
    for (Entry<String, String> e: m.entrySet()) {
      conf.set(key + "." + CONF + "." + e.getKey(), e.getValue());
    }
  }

  static String key(String... values) {
    StringBuilder sb = new StringBuilder(KEY_PREFIX);
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

  static List<Spec> specsFromConf(Configuration conf) {
    List<Spec> result = new ArrayList<Spec>();
    for (String spec : getEntries(conf, key(SPEC))) {
      String ifName = conf.get(key(SPEC, spec, CLASS));
      Map<String, String> specConf = getConf(conf, key(SPEC, spec));
      result.add(new Spec(spec, ifName, specConf));
    }
    return result;
  }

  static List<String> inputSpecsFromConf(Configuration conf) {
    String[] strings = conf.getStrings(key(INPUTSPECS));
    if (strings == null) {
      return emptyList();
    }
    return asList(strings);
  }

  static String outputSpecFromConf(Configuration conf) {
    return conf.get(key(OUTPUTSPEC));
  }

  static List<ClassDefinition> classDefinitionsFromConf(Configuration conf) {
    List<ClassDefinition> result = new ArrayList<ClassDefinition>();
    for (String inputformat : getEntries(conf, key(CLASS))) {
      String className = conf.get(key(CLASS, inputformat, NAME));
      String lib = conf.get(key(CLASS, inputformat, LIBRARY));
      Map<String, String> ifConf = getConf(conf, key(CLASS, inputformat));
      result.add(new ClassDefinition(inputformat, lib, className, ifConf));
    }
    return result;
  }

  static List<Library> librariesFromConf(Configuration conf) {
    if (conf == null) {
      throw new NullPointerException("conf");
    }
    List<Library> result = new ArrayList<Library>();
    for (String lib : getEntries(conf, key(LIBRARY))) {
      String[] paths = conf.getStrings(key(LIBRARY, lib, PATHS), "");
      if (paths == null || paths.length == 0) {
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
      String[] strings = new String[jars.size()];
      for (int i = 0; i < strings.length; i++) {
        strings[i] = jars.get(i).toString();

      }
      conf.setStrings(key(LIBRARY, library.getID(), PATHS), strings);
    }
  }

  public static void setClassDefinitions(Configuration conf, Collection<ClassDefinition> inputFormats) {
    for (ClassDefinition ifDef : inputFormats) {
      conf.set(key(CLASS, ifDef.getID(), NAME), ifDef.getClassName());
      if (ifDef.getLibraryID() != null) {
        conf.set(key(CLASS, ifDef.getID(), LIBRARY), ifDef.getLibraryID());
      }
      setConf(conf, key(CLASS, ifDef.getID()), ifDef.getConf());
    }
  }

  public static void setSpecs(Configuration conf, Collection<Spec> specs) {
    for (Spec spec : specs) {
      conf.set(key(SPEC, spec.getId(), CLASS), spec.getClassDefinitionID());
      setConf(conf, key(SPEC, spec.getId()), spec.getConf());
    }
  }

  public static void setInputSpecs(Configuration conf, String... specIds) {
    conf.setStrings(key(INPUTSPECS), specIds);
  }

  public static void setOutputSpec(Configuration conf, String specID) {
    conf.set(key(OUTPUTSPEC), specID);
  }

  private static Map<String, String> getConf(Configuration conf, String baseKey) {
    Map<String, String> result = new TreeMap<String, String>();
    for (String key : getEntriesFull(conf, baseKey + "." + CONF)) {
      result.put(key, conf.get(baseKey + "." + CONF + "." + key));
    }
    return result;
  }

}
