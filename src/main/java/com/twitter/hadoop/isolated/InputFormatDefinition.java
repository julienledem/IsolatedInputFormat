package com.twitter.hadoop.isolated;

import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.Map;

public final class InputFormatDefinition {

  static Map<String, String> toMap(String[] props) {
    Map<String, String> map = new HashMap<String, String>();
    for (String prop : props) {
      String[] kv = prop.split("=", 2);
      if (kv.length != 2) {
        throw new IllegalArgumentException(prop + " should be key=value");
      }
      map.put(kv[0], kv[1]);
    }
    return map;
  }

  private final String name;
  private final String libraryName;
  private final String inputFormatClassName;
  private final Map<String, String> conf;

  public InputFormatDefinition(String name, String libraryName,
      String inputFormatClassName, Map<String, String> conf) {
    super();
    this.name = name;
    this.libraryName = libraryName;
    this.inputFormatClassName = inputFormatClassName;
    this.conf = unmodifiableMap(new HashMap<String, String>(conf));
  }

  public InputFormatDefinition(String name, String libraryName,
      String inputFormatClassName, String... props) {
    this(name, libraryName, inputFormatClassName, toMap(props));
  }

  public String getName() {
    return name;
  }

  public String getLibraryName() {
    return libraryName;
  }

  public String getInputFormatClassName() {
    return inputFormatClassName;
  }

  public Map<String, String> getConf() {
    return conf;
  }

}