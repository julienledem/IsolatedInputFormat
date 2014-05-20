package com.twitter.hadoop.isolated;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;

public class IsolatedInputSplit extends InputSplit implements Writable, Configurable {

  private InputSplit delegate;
  private InputSpec inputSpec;
  transient private Configuration configuration;

  public IsolatedInputSplit() {
    // Writable
  }

  IsolatedInputSplit(InputSpec inputSpec, InputSplit delegate, Configuration configuration) {
    super();
    this.inputSpec = inputSpec;
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
    String inputFormatName = in.readUTF();
    int count = in.readInt();
    Map<String, String> conf = new HashMap<String, String>();
    for (int i = 0; i < count; i++) {
      conf.put(in.readUTF(), in.readUTF());
    }
    this.inputSpec = new InputSpec(inputFormatName, conf);
    System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&");
    System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&");
    System.out.println(configuration);
    System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&");
    System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&");
    this.delegate = deserialize(in, in.readUTF());
  }

  private <T extends InputSplit> T deserialize(DataInput in, String name) throws IOException {
    return IsolatedInputFormat.resolverFromConf(configuration).<T>deserializeSplit((InputStream)in, inputSpec, name, configuration);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.inputSpec.getInputFormatName());
    out.writeInt(this.inputSpec.getConf().size());
    for (Entry<String, String> e : inputSpec.getConf().entrySet()) {
      out.writeUTF(e.getKey());
      out.writeUTF(e.getValue());
    }
    out.writeUTF(this.delegate.getClass().getName());
    SerializationFactory factory = new SerializationFactory(configuration);
    Serializer serializer = factory.getSerializer(this.delegate.getClass());
    serializer.open((OutputStream)out);
    serializer.serialize(this.delegate);
  }

  InputSpec getInputSpec() {
    return inputSpec;
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

}