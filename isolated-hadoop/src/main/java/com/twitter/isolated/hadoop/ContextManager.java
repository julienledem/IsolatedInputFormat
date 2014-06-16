package com.twitter.isolated.hadoop;

import static com.twitter.isolated.hadoop.IsolatedConf.classDefinitionsFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.inputSpecsFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.librariesFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.outputSpecFromConf;
import static com.twitter.isolated.hadoop.IsolatedConf.setSpecs;
import static com.twitter.isolated.hadoop.IsolatedConf.specsFromConf;
import static com.twitter.isolated.hadoop.LibraryManager.getClassLoader;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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

  private static Configuration newConf(final Configuration conf, final Spec inputSpec, final ClassDefinition inputFormatDefinition) {
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
  private List<Spec> inputSpecs = new ArrayList<Spec>();
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
    for (Spec spec : specsFromConf(conf)) {
      lookup(classDefByName, spec.getClassDefinition()); // validate conf
      specByName.put(spec.getId(), spec);
    }
    List<String> inputSpecIDs = inputSpecsFromConf(conf);
    for (String inputSpecID : inputSpecIDs) {
      inputSpecs.add(getSpec(inputSpecID));
    }
    String outputSpecID = outputSpecFromConf(conf);
    if (outputSpecID != null) {
      outputSpec = getSpec(outputSpecID);
    }
  }

  /**
   * @return the configured input specs
   */
  public Collection<Spec> getInputSpecs() {
    return inputSpecs;
  }

  /**
   * @param id the id of the spec
   * @return the corresponding spec
   */
  public Spec getSpec(String id) {
    return lookup(specByName, id);
  }

  /**
   * @return the configured output spec
   */
  public Spec getOutputSpec() {
    return outputSpec;
  }

  /**
   * A task to be executed in the context of a Spec (Library + conf)
   *
   * @author Julien Le Dem
   *
   * @param <T> the returned type
   */
  public static abstract class ContextualCall<T> {
    private Configuration afterConf;
    public Spec spec;

    /**
     * To detect modifications to conf.
     * When an implementation modifies or wraps a conf, this should be the actual
     * object passed to the underlying implementation
     * @param conf the modified conf if not the original contextualConf
     */
    protected void setAfterConfiguration(Configuration conf) {
      this.afterConf = conf;
    }

    /**
     * will be called in the context of a conf and a Thread Context ClassLoader
     * @param contextualConf the conf resulting of the contextual overlay on the base conf
     * @return the result of the call
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract T call(Configuration contextualConf) throws IOException, InterruptedException;

  }

  /**
   * Will call and return the result of the ContextualCall in the context defined by the spec
   * @param specId the id of the spec defining the context
   * @param callable the task to call
   * @return the result of the call
   * @throws IOException
   */
  public <T> T callInContext(String specId, ContextualCall<T> callable) throws IOException {
    return callInContext(getSpec(specId), callable);
  }

  /**
   * guarantees that the delegated calls are done in the right context:
   *  - classloader to the proper lib
   *  - configuration from the proper Spec and ClassDefinition
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
      setSpecs(conf, asList(spec));
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

  /**
   * returns a new instance of the class in the context of the spec
   * @param specID defines the context
   * @param name the class name to instantiate
   * @param clazz the expected parent class
   * @return a new instance of class name
   * @throws IOException
   */
  public <T> T newInstance(String specID, final String name, final Class<T> clazz) throws IOException {
    return callInContext(specID, new ContextualCall<T>() {
      @Override
      public T call(Configuration contextualConf) throws IOException, InterruptedException {
        return newInstance(contextualConf, name, clazz);
      }
    });
  }

}
