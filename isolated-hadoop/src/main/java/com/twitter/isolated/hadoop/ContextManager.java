package com.twitter.isolated.hadoop;

import static com.twitter.isolated.hadoop.IsolatedConf.inputFormatDefinitionsFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.inputSpecsFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.key;
import static com.twitter.isolated.hadoop.IsolatedConf.librariesFromConf;
import static com.twitter.isolated.hadoop.LibraryManager.getClassLoader;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class ContextManager {

  private static <T> T lookup(Map<String, T> map, String key) {
    T lookedUp = map.get(key);
    if (lookedUp == null) {
      throw new IllegalArgumentException(key + " not found in " + map.keySet());
    }
    return lookedUp;
  }


  private static void applyConf(Configuration conf, Map<String, String> map) {
    for (Entry<String, String> entry : map.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
  }

  protected Configuration newConf(Configuration conf) {
    return new Configuration(conf);
  }

  Configuration newConf(final Configuration conf, final InputSpec inputSpec, final InputFormatDefinition inputFormatDefinition) {
    Configuration newConf = newConf(conf);
    applyConf(newConf, inputFormatDefinition.getConf());
    applyConf(newConf, inputSpec.getConf());
    return newConf;
  }

  public static void setInputSpecs(Configuration conf, Collection<InputSpec> inputSpecs) {
    for (InputSpec inputSpec : inputSpecs) {
      conf.set(key("inputspec", inputSpec.getId(), "inputformat"), inputSpec.getInputFormatName());
      setConf(conf, key("inputspec", inputSpec.getId()), inputSpec.getConf());
    }
  }

  public static void setConf(Configuration conf, String key, Map<String, String> m) {
    for (Entry<String, String> e: m.entrySet()) {
      conf.set(key + ".conf." + e.getKey(), e.getValue());
    }
  }

  protected Configuration conf;
  private Map<String, Library> libByName = new HashMap<String, Library>();
  private Map<String, InputFormatDefinition> inputFormatDefByName = new LinkedHashMap<String, InputFormatDefinition>();
  private Map<String, InputSpec> inputSpecByName = new LinkedHashMap<String, InputSpec>();
  private Map<String, ClassLoader> classLoaderByInputFormatName = new LinkedHashMap<String, ClassLoader>();

  public ContextManager(Configuration conf) {
    this.conf = conf;
    for (Library library : librariesFromConf(conf)) {
      libByName.put(library.getName(), library);
    }
    for (InputFormatDefinition inputFormatDef : inputFormatDefinitionsFromConf(conf)) {
      inputFormatDefByName.put(inputFormatDef.getName(), inputFormatDef);
      Library library = inputFormatDef.getLibraryName() == null ? null : lookup(libByName, inputFormatDef.getLibraryName());
      ClassLoader classLoader = getClassLoader(library);
      classLoaderByInputFormatName.put(inputFormatDef.getName(), classLoader);
    }
    for (InputSpec spec : inputSpecsFromConf(conf)) {
      lookup(inputFormatDefByName, spec.getInputFormatName()); // validate conf
      inputSpecByName.put(spec.getId(), spec);
    }
  }

  public Collection<InputSpec> getInputSpecs() {
    return inputSpecByName.values();
  }

  public InputSpec getInputSpec(String id) {
    return lookup(inputSpecByName, id);
  }

  public static abstract class ContextualCall<T> {
    protected final InputSpec inputSpec;

    public ContextualCall(InputSpec inputSpec) {
      this.inputSpec = inputSpec;
    }

    public abstract T call(Configuration conf) throws IOException, InterruptedException;

    protected final void checkConfUpdate(Configuration before, Configuration after, Configuration conf) {
      for (Entry<String, String> e : after) {
        String previous = before.getRaw(e.getKey());
        if (previous == null || !previous.equals(e.getValue())) {
          inputSpec.getConf().put(e.getKey(), e.getValue());
        }
        setInputSpecs(conf, asList(inputSpec));
      }
    }

  }

  /**
   * guarantees that the delegated calls are done in the right context:
   *  - classloader to the proper lib
   *  - configuration from the proper inputSpec and InputFormat
   *  - configuration modifications are propagated in isolation
   * @param callable what to do
   * @return what callable returns
   * @throws IOException
   */
  public <T> T callInContext(ContextualCall<T> callable) throws IOException {
    Thread currentThread = Thread.currentThread();
    ClassLoader contextClassLoader = currentThread.getContextClassLoader();
    try {
      InputSpec inputSpec = callable.inputSpec;
      InputFormatDefinition inputFormatDefinition = lookup(inputFormatDefByName, inputSpec.getInputFormatName());
      currentThread.setContextClassLoader(lookup(classLoaderByInputFormatName, inputSpec.getInputFormatName()));
      Configuration newConf = newConf(conf, inputSpec, inputFormatDefinition);
      return callable.call(newConf);
    } catch (InterruptedException e) {
      throw new IOException("thread interrupted", e);
    } finally {
      currentThread.setContextClassLoader(contextClassLoader);
    }
  }

  protected <T> T newInputFormat(Configuration contextualConf, InputSpec inputSpec, Class<T> parentClass) {
    InputFormatDefinition ifdef = lookup(inputFormatDefByName, inputSpec.getInputFormatName());
    return newInstance(contextualConf, ifdef.getInputFormatClassName(), parentClass);
  }


  protected <T> T newInstance(Configuration contextualConf, String className, Class<T> parentClass) {
    try {
      Class<?> clazz = contextualConf.getClassByName(className);
      return parentClass.cast(ReflectionUtils.newInstance(clazz, contextualConf));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public <T> T newInstance(String inputSpecID, final String name, final Class<T> clazz) throws IOException {
    return callInContext(new ContextualCall<T>(getInputSpec(inputSpecID)) {
      @Override
      public T call(Configuration contextualConf) throws IOException, InterruptedException {
        return newInstance(contextualConf, name, clazz);
      }
    });
  }

}
