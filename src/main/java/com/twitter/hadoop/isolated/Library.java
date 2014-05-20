package com.twitter.hadoop.isolated;

import static java.util.Collections.unmodifiableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.Path;

public final class Library {
  private final String name;
  private final List<Path> jars;

  public Library(String name, Collection<Path> jars) {
    super();
    this.name = name;
    this.jars = unmodifiableList(new ArrayList<Path>(jars));
  }

  public String getName() {
    return name;
  }

  public List<Path> getJars() {
    return jars;
  }
}