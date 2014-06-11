package com.twitter.isolated.hadoop;

import static com.twitter.isolated.hadoop.IsolatedConf.classDefinitionsFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.inputSpecsFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.librariesFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.outputSpecFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.setInputSpecs;
import static com.twitter.isolated.hadoop.LibraryManager.getClassLoader;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Manages calling methods of an {@link InputFormat} or {@link OutputFormat} in the context of
 * a {@link Configuration} and a {@link ClassLoader} based on a {@link Spec}
 *
 * @author Julien Le Dem
 *
 */
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

  private Configuration newConf(final Configuration conf, final Spec inputSpec, final ClassDefinition inputFormatDefinition) {
    Configuration newConf = new Configuration(conf);
    applyConf(newConf, inputFormatDefinition.getConf());
    applyConf(newConf, inputSpec.getConf());
    return newConf;
  }

  protected Configuration conf;
  private Map<String, Library> libByName = new HashMap<String, Library>();
  private Map<String, ClassDefinition> classDefByName = new LinkedHashMap<String, ClassDefinition>();
  private Map<String, Spec> specByName = new LinkedHashMap<String, Spec>();
  private Map<String, ClassLoader> classLoaderByInputFormatName = new LinkedHashMap<String, ClassLoader>();
  private Spec outputSpec;

  public ContextManager(Configuration conf) {
    this.conf = conf;
    for (Library library : librariesFromConf(conf)) {
      libByName.put(library.getName(), library);
    }
    for (ClassDefinition inputFormatDef : classDefinitionsFromConf(conf)) {
      classDefByName.put(inputFormatDef.getName(), inputFormatDef);
      Library library = inputFormatDef.getLibraryName() == null ? null : lookup(libByName, inputFormatDef.getLibraryName());
      ClassLoader classLoader = getClassLoader(library);
      classLoaderByInputFormatName.put(inputFormatDef.getName(), classLoader);
    }
    for (Spec spec : inputSpecsFromConf(conf)) {
      lookup(classDefByName, spec.getClassDefinition()); // validate conf
      specByName.put(spec.getId(), spec);
    }
    outputSpec = outputSpecFromConf(conf);
    lookup(classDefByName, outputSpec.getClassDefinition()); // validate conf
  }

  public Collection<Spec> getInputSpecs() {
    return specByName.values();
  }

  public Spec getInputSpec(String id) {
    return lookup(specByName, id);
  }

  public Spec getOutputSpec() {
    return outputSpec;
  }

  public static abstract class ContextualCall<T> {
    private Configuration afterConf;
    public Spec spec;

    /**
     * to detect modifications to conf.
     * @param conf the modified conf if not the original contextualConf
     */
    protected void setAfterConfiguration(Configuration conf) {
      this.afterConf = conf;
    }

    public abstract T call(Configuration contextualConf) throws IOException, InterruptedException;

  }

  public <T> T callInContext(String specId, ContextualCall<T> callable) throws IOException {
    return callInContext(getInputSpec(specId), callable);
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
  public <T> T callInContext(Spec spec, ContextualCall<T> callable) throws IOException {
    Thread currentThread = Thread.currentThread();
    ClassLoader contextClassLoader = currentThread.getContextClassLoader();
    try {
      ClassDefinition inputFormatDefinition = lookup(classDefByName, spec.getClassDefinition());
      currentThread.setContextClassLoader(lookup(classLoaderByInputFormatName, spec.getClassDefinition()));
      Configuration before = newConf(conf, spec, inputFormatDefinition);
      callable.afterConf = new Configuration(before);
      callable.spec = spec;
      T result = callable.call(callable.afterConf);
      callable.spec = null;
      // detect changes in the conf and save them
      for (Entry<String, String> e : callable.afterConf) {
        String previous = before.getRaw(e.getKey());
        if (previous == null || !previous.equals(e.getValue())) {
          spec.getConf().put(e.getKey(), e.getValue());
        }
      }
      setInputSpecs(conf, asList(spec));
      return result;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("thread interrupted", e);
    } finally {
      currentThread.setContextClassLoader(contextClassLoader);
    }
  }

  protected <T> T newInstanceFromSpec(Configuration contextualConf, Spec spec, Class<T> parentClass) {
    try {
      ClassDefinition classdef = lookup(classDefByName, spec.getClassDefinition());
      return newInstance(contextualConf, classdef.getClassName(), parentClass);
    } catch (RuntimeException e) {
      throw new RuntimeException("Can't instantiate class from spec " + spec, e);
    }
  }

  protected <T> T newInstance(Configuration contextualConf, String className, Class<T> parentClass) {
    try {
      Class<?> clazz = contextualConf.getClassByName(className);
      return parentClass.cast(ReflectionUtils.newInstance(clazz, contextualConf));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public <T> T newInstance(String specID, final String name, final Class<T> clazz) throws IOException {
    return callInContext(specID, new ContextualCall<T>() {
      @Override
      public T call(Configuration contextualConf) throws IOException, InterruptedException {
        return newInstance(contextualConf, name, clazz);
      }
    });
  }

}
