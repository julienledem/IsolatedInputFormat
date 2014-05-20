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
}