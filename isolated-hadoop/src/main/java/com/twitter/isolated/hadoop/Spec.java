package com.twitter.isolated.hadoop;

import java.util.Map;

public final class Spec {
  private final String id;
  private final String classDefinition;
  private final Map<String, String> conf;

  public Spec(String id, String classDefinition, Map<String, String> conf) {
    super();
    this.id = id;
    this.classDefinition = classDefinition;
    this.conf = conf;
  }

  public Spec(String id, String classDefinition, String... props) {
    this(id, classDefinition, ClassDefinition.toMap(props));
  }

  public String getClassDefinition() {
    return classDefinition;
  }

  public Map<String, String> getConf() {
    return conf;
  }

  public String getId() {
    return id;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((conf == null) ? 0 : conf.hashCode());
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    result = prime * result
        + ((classDefinition == null) ? 0 : classDefinition.hashCode());
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
    Spec other = (Spec) obj;
    if (conf == null) {
      if (other.conf != null)
        return false;
    } else if (!conf.equals(other.conf))
      return false;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    if (classDefinition == null) {
      if (other.classDefinition != null)
        return false;
    } else if (!classDefinition.equals(other.classDefinition))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Spec [id=" + id + ", classDefinition=" + classDefinition
        + ", conf=" + conf + "]";
  }

}