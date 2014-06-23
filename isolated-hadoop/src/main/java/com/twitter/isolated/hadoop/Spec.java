package com.twitter.isolated.hadoop;

import java.util.Map;

public final class Spec {
  private final String id;
  private final String classDefinitionID;
  private final Map<String, String> conf;

  public Spec(String id, String classDefinitionID, Map<String, String> conf) {
    super();
    this.id = id;
    this.classDefinitionID = classDefinitionID;
    this.conf = conf;
  }

  public Spec(String id, String classDefinition, String... props) {
    this(id, classDefinition, ClassDefinition.toMap(props));
  }

  public String getClassDefinitionID() {
    return classDefinitionID;
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
        + ((classDefinitionID == null) ? 0 : classDefinitionID.hashCode());
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
    if (classDefinitionID == null) {
      if (other.classDefinitionID != null)
        return false;
    } else if (!classDefinitionID.equals(other.classDefinitionID))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Spec [id=" + id + ", classDefinitionID=" + classDefinitionID
        + ", conf=" + conf + "]";
  }

}