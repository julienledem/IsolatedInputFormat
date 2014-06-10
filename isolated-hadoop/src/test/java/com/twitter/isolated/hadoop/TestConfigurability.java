package com.twitter.isolated.hadoop;

import static com.twitter.isolated.hadoop.IsolatedConf.classDefinitionsFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.inputSpecsFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.librariesFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.setClassDefinitions;
import static com.twitter.isolated.hadoop.IsolatedConf.setInputSpecs;
import static com.twitter.isolated.hadoop.IsolatedConf.setLibraries;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestConfigurability {
  @Test
  public void testConf() throws IOException {
    Configuration conf = new Configuration();
    List<Library> libs = asList(
        new Library("parquet-lib", new Path("foo")),
        new Library("hadoop-lib") // empty
        );
    setLibraries(conf, libs);
    List<Library> librariesFromConf = librariesFromConf(conf);
    assertEquals(sortLibs(libs), sortLibs(librariesFromConf));

    List<ClassDefinition> ifs = asList(
        new ClassDefinition("parquet-inputformat", "parquet-lib", "parquet.hadoop.ParquetInputFormat", "parquet.read.support.class=parquet.hadoop.example.GroupReadSupport"),
        new ClassDefinition("text-inputformat", "hadoop-lib", "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")
        );
    setClassDefinitions(conf, ifs);
    List<ClassDefinition> inputFormatDefinitionsFromConf = classDefinitionsFromConf(conf);
    assertEquals(sortIFs(ifs), sortIFs(inputFormatDefinitionsFromConf));

    List<Spec> inputSpecs = asList(
        new Spec("0", "parquet-inputformat", "mapred.input.dir=/foo/bar/1"),
        new Spec("1", "text-inputformat", "mapred.input.dir=/foo/bar/2")
        );
    setInputSpecs(conf, inputSpecs);
    List<Spec> inputSpecsFromConf = inputSpecsFromConf(conf);
    assertEquals(sortISs(inputSpecs), sortISs(inputSpecsFromConf));
  }

  private List<Spec> sortISs(List<Spec> iss) {
    Collections.sort(iss, new Comparator<Spec>() {
      @Override
      public int compare(Spec l1, Spec l2) {
        return l1.getId().compareTo(l2.getId());
      }
    });
    return iss;
  }

  private List<ClassDefinition> sortIFs(List<ClassDefinition> ifs) {
    Collections.sort(ifs, new Comparator<ClassDefinition>() {
      @Override
      public int compare(ClassDefinition l1, ClassDefinition l2) {
        return l1.getName().compareTo(l2.getName());
      }
    });
    return ifs;
  }

  private List<Library> sortLibs(List<Library> libs) {
    Collections.sort(libs, new Comparator<Library>() {
      @Override
      public int compare(Library l1, Library l2) {
        return l1.getName().compareTo(l2.getName());
      }
    });
    return libs;
  }
}
