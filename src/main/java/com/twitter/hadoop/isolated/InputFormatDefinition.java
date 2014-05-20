package com.twitter.hadoop.isolated;

import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public final class InputFormatDefinition {

  static Map<String, String> toMap(String[] props) {
    Map<String, String> map = new TreeMap<String, String>();
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

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((conf == null) ? 0 : conf.hashCode());
    result = prime
        * result
        + ((inputFormatClassName == null) ? 0 : inputFormatClassName.hashCode());
    result = prime * result
        + ((libraryName == null) ? 0 : libraryName.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    InputFormatDefinition other = (InputFormatDefinition) obj;
    if (conf == null) {
      if (other.conf != null)
        return false;
    } else if (!conf.equals(other.conf))
      return false;
    if (inputFormatClassName == null) {
      if (other.inputFormatClassName != null)
        return false;
    } else if (!inputFormatClassName.equals(other.inputFormatClassName))
      return false;
    if (libraryName == null) {
      if (other.libraryName != null)
        return false;
    } else if (!libraryName.equals(other.libraryName))
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "InputFormatDefinition [name=" + name + ", libraryName="
        + libraryName + ", inputFormatClassName=" + inputFormatClassName
        + ", conf=" + conf + "]";
  }



}