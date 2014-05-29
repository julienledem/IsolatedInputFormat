package com.twitter.isolated.hadoop;

import java.util.Map;

public final class InputSpec {
  private final String id;
  private final String inputFormatName;
  private final Map<String, String> conf;

  public InputSpec(String id, String inputFormatName, Map<String, String> conf) {
    super();
    this.id = id;
    this.inputFormatName = inputFormatName;
    this.conf = conf;
  }

  public InputSpec(String id, String inputFormatName, String... props) {
    this(id, inputFormatName, InputFormatDefinition.toMap(props));
  }

  public String getInputFormatName() {
    return inputFormatName;
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
        + ((inputFormatName == null) ? 0 : inputFormatName.hashCode());
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
    InputSpec other = (InputSpec) obj;
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
    if (inputFormatName == null) {
      if (other.inputFormatName != null)
        return false;
    } else if (!inputFormatName.equals(other.inputFormatName))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "InputSpec [id=" + id + ", inputFormatName=" + inputFormatName
        + ", conf=" + conf + "]";
  }

}