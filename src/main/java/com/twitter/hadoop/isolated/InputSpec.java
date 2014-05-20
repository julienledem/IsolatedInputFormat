package com.twitter.hadoop.isolated;

import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.Map;

public final class InputSpec {
  private final String inputFormatName;
  private final Map<String, String> conf;

  public InputSpec(String inputFormatName, Map<String, String> conf) {
    super();
    this.inputFormatName = inputFormatName;
    this.conf = unmodifiableMap(new HashMap<String, String>(conf));
  }

  public InputSpec(String inputFormatName, String... props) {
    this(inputFormatName, InputFormatDefinition.toMap(props));
  }

  public String getInputFormatName() {
    return inputFormatName;
  }

  public Map<String, String> getConf() {
    return conf;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((conf == null) ? 0 : conf.hashCode());
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
    if (inputFormatName == null) {
      if (other.inputFormatName != null)
        return false;
    } else if (!inputFormatName.equals(other.inputFormatName))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "InputSpec [inputFormatName=" + inputFormatName + ", conf=" + conf
        + "]";
  }


}