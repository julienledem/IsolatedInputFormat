package com.twitter.hadoop.isolated;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.Path;

public final class Library {
  private final String name;
  private final List<Path> jars;

  public Library(String name, Path... jars) {
    this(name, asList(jars));
  }

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

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((jars == null) ? 0 : jars.hashCode());
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
    Library other = (Library) obj;
    if (jars == null) {
      if (other.jars != null)
        return false;
    } else if (!jars.equals(other.jars))
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
    return "Library [name=" + name + ", jars=" + jars + "]";
  }

}