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

public class IsolatedScheme<K, V> extends Scheme<JobConf, RecordReader<K, V>,OutputCollector<K,V>,Object[],Void> {
  private static final long serialVersionUID = 1;

  @Override
  public void sink(FlowProcess<JobConf> fp, SinkCall<Void, OutputCollector<K,V>> sc) throws IOException {
    TupleEntry tuple = sc.getOutgoingEntry();

    if (tuple.size() != 2) {
      throw new RuntimeException("IsolatedScheme expects tuples with an arity of exactly 2, but found " + tuple.getFields());
    }

    K key = (K)tuple.getObject(0);
    V value = (V)tuple.getObject(1);
    OutputCollector<K, V> output = sc.getOutput();
    output.collect(key, value);
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader<K,V>, OutputCollector<K,V>> tap, JobConf jobConf) {
//    jobConf.setOutputFormat(IsolatedOutputFormat.class);
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean source(FlowProcess<JobConf> fp, SourceCall<Object[], RecordReader<K,V>> sc) throws IOException {
    K key = sc.getInput().createKey();
    V value = sc.getInput().createValue();
    boolean hasNext = sc.getInput().next(key, value);
    if (!hasNext) { return false; }
    sc.getIncomingEntry().setTuple(new Tuple(key, value));
    return true;
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader<K,V>, OutputCollector<K,V>> t, JobConf jobConf) {
    jobConf.setInputFormat(IsolatedInputFormat.class);
  }

}
