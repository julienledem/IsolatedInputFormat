package com.twitter.isolated.hadoop;

import static com.twitter.isolated.hadoop.IsolatedConf.inputFormatDefinitionsFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.inputSpecsFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.librariesFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.setInputFormats;
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

    List<InputFormatDefinition> ifs = asList(
        new InputFormatDefinition("parquet-inputformat", "parquet-lib", "parquet.hadoop.ParquetInputFormat", "parquet.read.support.class=parquet.hadoop.example.GroupReadSupport"),
        new InputFormatDefinition("text-inputformat", "hadoop-lib", "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")
        );
    setInputFormats(conf, ifs);
    List<InputFormatDefinition> inputFormatDefinitionsFromConf = inputFormatDefinitionsFromConf(conf);
    assertEquals(sortIFs(ifs), sortIFs(inputFormatDefinitionsFromConf));

    List<InputSpec> inputSpecs = asList(
        new InputSpec("0", "parquet-inputformat", "mapred.input.dir=/foo/bar/1"),
        new InputSpec("1", "text-inputformat", "mapred.input.dir=/foo/bar/2")
        );
    setInputSpecs(conf, inputSpecs);
    List<InputSpec> inputSpecsFromConf = inputSpecsFromConf(conf);
    assertEquals(sortISs(inputSpecs), sortISs(inputSpecsFromConf));
  }

  private List<InputSpec> sortISs(List<InputSpec> iss) {
    Collections.sort(iss, new Comparator<InputSpec>() {
      @Override
      public int compare(InputSpec l1, InputSpec l2) {
        return l1.getId().compareTo(l2.getId());
      }
    });
    return iss;
  }

  private List<InputFormatDefinition> sortIFs(List<InputFormatDefinition> ifs) {
    Collections.sort(ifs, new Comparator<InputFormatDefinition>() {
      @Override
      public int compare(InputFormatDefinition l1, InputFormatDefinition l2) {
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
