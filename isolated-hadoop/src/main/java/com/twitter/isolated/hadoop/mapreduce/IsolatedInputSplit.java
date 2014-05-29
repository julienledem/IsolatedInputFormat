package com.twitter.isolated.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;

public class IsolatedInputSplit extends InputSplit implements Writable, Configurable {

  private InputSplit delegate;
  private String inputSpecID;
  transient private Configuration configuration;

  public IsolatedInputSplit() {
    // Writable
  }

  IsolatedInputSplit(String inputSpecID, InputSplit delegate, Configuration configuration) {
    super();
    this.inputSpecID = inputSpecID;
    this.delegate = delegate;
    this.configuration = configuration;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return delegate.getLength();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return delegate.getLocations();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.inputSpecID = in.readUTF();
    this.delegate = deserialize((InputStream)in, in.readUTF());
  }

  private InputSplit deserialize(InputStream in, String name) throws IOException {
    return new MapreduceContextManager(configuration).deserializeSplit(in, inputSpecID, name, configuration);
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    out.writeUTF(this.inputSpecID);
    Class<?> delegateClass = this.delegate.getClass();
    out.writeUTF(delegateClass.getName());
    serializeDelegate(new AdapterOutput(out), delegateClass);
  }

  private <T> void serializeDelegate(OutputStream out, Class<T> delegateClass) throws IOException {
    // TODO: this should happen in context of the classloader and conf
    SerializationFactory factory = new SerializationFactory(configuration);
    Serializer<T> serializer = factory.getSerializer(delegateClass);
    serializer.open(out);
    serializer.serialize(delegateClass.cast(this.delegate));
  }

  String getInputSpecID() {
    return inputSpecID;
  }

  InputSplit getDelegate() {
    return delegate;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
    try {
      Configuration.dumpConfiguration(configuration, new OutputStreamWriter(System.out));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static final class AdapterOutput extends OutputStream {
    private final DataOutput out;

    private AdapterOutput(DataOutput out) {
      this.out = out;
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }
  }

}