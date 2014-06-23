package com.twitter.isolated.hadoop;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.Path;

public final class Library {
  private final String id;
  private final List<Path> jars;

  public Library(String id, Path... jars) {
    this(id, asList(jars));
  }

  public Library(String id, Collection<Path> jars) {
    super();
    this.id = id;
    this.jars = unmodifiableList(new ArrayList<Path>(jars));
  }

  public String getID() {
    return id;
  }

  public List<Path> getJars() {
    return jars;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((jars == null) ? 0 : jars.hashCode());
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
    Library other = (Library) obj;
    if (jars == null) {
      if (other.jars != null)
        return false;
    } else if (!jars.equals(other.jars))
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
    return "Library [name=" + id + ", jars=" + jars + "]";
  }

}