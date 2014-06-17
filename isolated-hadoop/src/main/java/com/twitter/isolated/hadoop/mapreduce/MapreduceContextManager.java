package com.twitter.isolated.hadoop.mapreduce;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.twitter.isolated.hadoop.ContextManager;

class MapreduceContextManager extends ContextManager {

  MapreduceContextManager(JobContext context) {
    this(context.getConfiguration());
  }

  MapreduceContextManager(Configuration conf) {
    super(conf);
  }

  // methods that make sure delegated calls are executed in the right context

  InputSplit deserializeSplit(final InputStream in, String inputSpecID, final String name) throws IOException {
    return callInContext(inputSpecID, new ContextualCall<InputSplit>() {
      public InputSplit call(CallContext ctxt) throws IOException,
          InterruptedException {
        try {
          Class<? extends InputSplit> splitClass = ctxt.localConf().getClassByName(name).asSubclass(InputSplit.class);
          return deserialize(in, ctxt.localConf(), splitClass);
        } catch (ClassNotFoundException e) {
          throw new IOException("could not resolve class " + name, e);
        }
      }

      // we need to define a common T for these calls to work together
      private <T extends InputSplit> InputSplit deserialize(final InputStream in, Configuration localConf, Class<T> splitClass) throws IOException {
        T delegateInstance = ReflectionUtils.newInstance(splitClass, localConf);
        SerializationFactory factory = new SerializationFactory(localConf);
        Deserializer<T> deserializer = factory.getDeserializer(splitClass);
        deserializer.open(in);
        return deserializer.deserialize(delegateInstance);
      }
    });
  }

}
