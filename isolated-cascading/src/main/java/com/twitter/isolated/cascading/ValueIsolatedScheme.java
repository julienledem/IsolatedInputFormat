package com.twitter.isolated.cascading;

import java.io.IOException;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ValueIsolatedScheme<K, V> extends IsolatedScheme<K, V> {
  private static final long serialVersionUID = 1;

  @Override
  void writeTupleToOutput(TupleEntry tuple, OutputCollector<K, V> output) throws IOException {
    if (tuple.size() != 1) {
      throw new RuntimeException("IsolatedScheme expects tuples with an arity of exactly 1, but found " + tuple.getFields());
    }
    @SuppressWarnings("unchecked") // nothing we can do here
    V value = (V)tuple.getObject(0);
    output.collect(null, value);
  }

  @Override
  Tuple readNextTupleFromInput(RecordReader<K, V> input) throws IOException {
    K key = input.createKey();
    V value = input.createValue();
    boolean hasNext = input.next(key, value);
    return hasNext ? new Tuple(value) : null;
  }

}
