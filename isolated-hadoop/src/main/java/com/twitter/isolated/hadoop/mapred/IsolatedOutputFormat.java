package com.twitter.isolated.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public class IsolatedOutputFormat<K, V> implements OutputFormat<K, V> {

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf jobConf) throws IOException {
    new MapredContextManager(jobConf).checkOutputSpecs(ignored);
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(FileSystem fs, JobConf jobConf, String name, Progressable p) throws IOException {
     return new MapredContextManager(jobConf).getRecordWriter(fs, name, p);
  }

}
