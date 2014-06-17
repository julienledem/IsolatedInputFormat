package com.twitter.isolated.hadoop;

import static java.util.Collections.unmodifiableMap;

import java.util.Map;
import java.util.TreeMap;

public final class ClassDefinition {

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

  private final String id;
  private final String libraryId;
  private final String className;
  private final Map<String, String> conf;

  public ClassDefinition(String id, String libraryId, String className, Map<String, String> conf) {
    super();
    this.id = id;
    this.libraryId = libraryId;
    this.className = className;
    this.conf = unmodifiableMap(new TreeMap<String, String>(conf));
  }

  public ClassDefinition(String name, String libraryName,
      String className, String... props) {
    this(name, libraryName, className, toMap(props));
  }

  public String getID() {
    return id;
  }

  public String getLibraryID() {
    return libraryId;
  }

  public String getClassName() {
    return className;
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
        + ((className == null) ? 0 : className.hashCode());
    result = prime * result
        + ((libraryId == null) ? 0 : libraryId.hashCode());
    result = prime * result + ((id == null) ? 0 : id.hashCode());
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
    ClassDefinition other = (ClassDefinition) obj;
    if (conf == null) {
      if (other.conf != null)
        return false;
    } else if (!conf.equals(other.conf))
      return false;
    if (className == null) {
      if (other.className != null)
        return false;
    } else if (!className.equals(other.className))
      return false;
    if (libraryId == null) {
      if (other.libraryId != null)
        return false;
    } else if (!libraryId.equals(other.libraryId))
      return false;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "ClassDefinition [id=" + id + ", libraryID="
        + libraryId + ", className=" + className
        + ", conf=" + conf + "]";
  }



}