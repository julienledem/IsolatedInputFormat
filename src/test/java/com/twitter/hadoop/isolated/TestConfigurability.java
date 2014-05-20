package com.twitter.hadoop.isolated;

import static com.twitter.hadoop.isolated.IsolatedInputFormat.inputFormatDefinitionsFromConf;
import static com.twitter.hadoop.isolated.IsolatedInputFormat.inputSpecsFromConf;
import static com.twitter.hadoop.isolated.IsolatedInputFormat.librariesFromConf;
import static com.twitter.hadoop.isolated.IsolatedInputFormat.setInputFormats;
import static com.twitter.hadoop.isolated.IsolatedInputFormat.setInputSpecs;
import static com.twitter.hadoop.isolated.IsolatedInputFormat.setLibraries;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

public class TestConfigurability {
  @Test
  public void testConf() throws IOException {
    Job job = new Job();
    List<Library> libs = asList(
        new Library("parquet-lib", new Path("foo")),
        new Library("hadoop-lib") // empty
        );
    setLibraries(job, libs);
    List<Library> librariesFromConf = librariesFromConf(job.getConfiguration());
    assertEquals(sortLibs(libs), sortLibs(librariesFromConf));

    List<InputFormatDefinition> ifs = asList(
        new InputFormatDefinition("parquet-inputformat", "parquet-lib", "parquet.hadoop.ParquetInputFormat", "parquet.read.support.class=parquet.hadoop.example.GroupReadSupport"),
        new InputFormatDefinition("text-inputformat", "hadoop-lib", "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")
        );
    setInputFormats(job, ifs);
    List<InputFormatDefinition> inputFormatDefinitionsFromConf = inputFormatDefinitionsFromConf(job.getConfiguration());
    assertEquals(sortIFs(ifs), sortIFs(inputFormatDefinitionsFromConf));

    List<InputSpec> inputSpecs = asList(
        new InputSpec("parquet-inputformat", "mapred.input.dir=/foo/bar/1"),
        new InputSpec("text-inputformat", "mapred.input.dir=/foo/bar/2")
        );
    setInputSpecs(job, inputSpecs);
    List<InputSpec> inputSpecsFromConf = inputSpecsFromConf(job.getConfiguration());
    assertEquals(sortISs(inputSpecs), sortISs(inputSpecsFromConf));
  }

  private List<InputSpec> sortISs(List<InputSpec> iss) {
    Collections.sort(iss, new Comparator<InputSpec>() {
      @Override
      public int compare(InputSpec l1, InputSpec l2) {
        return l1.getInputFormatName().compareTo(l2.getInputFormatName());
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
