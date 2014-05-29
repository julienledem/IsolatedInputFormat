package com.twitter.isolated.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

public class IsolatedInputSplit implements InputSplit, JobConfigurable {

  private String inputSpecID;
  private InputSplit delegate;
  transient private JobConf configuration;

  public IsolatedInputSplit() {
    // for deserialization
  }

  public IsolatedInputSplit(String inputSpecID, InputSplit delegate, JobConf configuration) {
    this.inputSpecID = inputSpecID;
    this.delegate = delegate;
    this.configuration = configuration;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    this.inputSpecID = input.readUTF();
    String name = input.readUTF();
    this.delegate = new MapredContextManager(configuration).newInstance(inputSpecID, name, InputSplit.class);
    delegate.readFields(input);
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeUTF(this.inputSpecID);
    output.writeUTF(this.delegate.getClass().getName());
    delegate.write(output);
  }

  @Override
  public long getLength() throws IOException {
    return delegate.getLength();
  }

  @Override
  public String[] getLocations() throws IOException {
    return delegate.getLocations();
  }

  public InputSplit getDelegate() {
    return delegate;
  }

  public String getInputSpecID() {
    return inputSpecID;
  }

  @Override
  public void configure(JobConf conf) {
    this.configuration = conf;
  }
}
