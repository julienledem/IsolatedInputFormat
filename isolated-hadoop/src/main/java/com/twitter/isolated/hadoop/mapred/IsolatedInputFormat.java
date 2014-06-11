package com.twitter.isolated.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class IsolatedInputFormat<K, V> implements InputFormat<K, V>{

  @Override
  public InputSplit[] getSplits(JobConf jobConf, final int numSplits) throws IOException {
    return new MapredContextManager(jobConf).getSplits(numSplits);
  }

  @Override
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf jobConf, Reporter reporter) throws IOException {
    return new MapredContextManager(jobConf).getRecordReader(split, reporter);
  }

}
