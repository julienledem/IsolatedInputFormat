package com.twitter.isolated.cascading;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.twitter.isolated.hadoop.mapred.IsolatedInputFormat;
import com.twitter.isolated.hadoop.mapred.IsolatedOutputFormat;

abstract public class IsolatedScheme<K, V> extends Scheme<JobConf, RecordReader<K, V>,OutputCollector<K,V>,Object[],Void> {
  private static final long serialVersionUID = 1;

  @Override
  public void sink(FlowProcess<JobConf> fp, SinkCall<Void, OutputCollector<K,V>> sc) throws IOException {
    TupleEntry tuple = sc.getOutgoingEntry();
    writeTupleToOutput(tuple, sc.getOutput());
  }

  abstract void writeTupleToOutput(TupleEntry tuple, OutputCollector<K, V> output) throws IOException;

  @Override
  public void sinkConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader<K,V>, OutputCollector<K,V>> tap, JobConf jobConf) {
    jobConf.setOutputFormat(IsolatedOutputFormat.class);
  }

  @Override
  public boolean source(FlowProcess<JobConf> fp, SourceCall<Object[], RecordReader<K,V>> sc) throws IOException {
    Tuple next = readNextTupleFromInput(sc.getInput());
    if (next != null) {
      sc.getIncomingEntry().setTuple(next);
      return true;
    } else {
      return false;
    }
  }

  abstract Tuple readNextTupleFromInput(RecordReader<K, V> input) throws IOException;

  @Override
  public void sourceConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader<K,V>, OutputCollector<K,V>> t, JobConf jobConf) {
    jobConf.setInputFormat(IsolatedInputFormat.class);
  }

}
