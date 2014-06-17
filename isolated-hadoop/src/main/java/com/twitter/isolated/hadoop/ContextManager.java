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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
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

  protected final Configuration globalConf;
  private final Map<String, Library> libByName = new HashMap<String, Library>();
  private final Map<String, ClassDefinition> classDefByName = new LinkedHashMap<String, ClassDefinition>();
  private final Map<String, Spec> specByName = new LinkedHashMap<String, Spec>();
  private final Map<String, ClassLoader> classLoaderByInputFormatName = new LinkedHashMap<String, ClassLoader>();
  private final List<Spec> inputSpecs;
  private final Spec outputSpec;

  public ContextManager(Configuration conf) {
    this.globalConf = conf;
    for (Library library : librariesFromConf(conf)) {
      libByName.put(library.getID(), library);
    }
    for (ClassDefinition inputFormatDef : classDefinitionsFromConf(conf)) {
      classDefByName.put(inputFormatDef.getID(), inputFormatDef);
      Library library = inputFormatDef.getLibraryID() == null ? null : lookup(libByName, inputFormatDef.getLibraryID());
      ClassLoader classLoader = getClassLoader(library);
      classLoaderByInputFormatName.put(inputFormatDef.getID(), classLoader);
    }
    for (Spec spec : specsFromConf(conf)) {
      lookup(classDefByName, spec.getClassDefinitionID()); // validate conf
      specByName.put(spec.getId(), spec);
    }
    List<Spec> inputs = new ArrayList<Spec>();
    List<String> inputSpecIDs = inputSpecsFromConf(conf);
    for (String inputSpecID : inputSpecIDs) {
      inputs.add(getSpec(inputSpecID));
    }
    inputSpecs = Collections.unmodifiableList(inputs);
    String outputSpecID = outputSpecFromConf(conf);
    outputSpec = outputSpecID == null ? null : getSpec(outputSpecID);
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

  public static class CallContext {

    private Configuration localConf;
    public final Spec spec;
    private final ContextManager contextManager;

    /**
     * to enable nesting context and propagating conf modifications
     * @param that the existing context
     * @param newLocalConf the new conf
     */
    public CallContext(CallContext that, Configuration newLocalConf) {
      this(that.contextManager, that.spec, newLocalConf);
      that.propagateConfigurationChanges(newLocalConf);
    }

    private CallContext(ContextManager contextManager, Spec spec, Configuration contextualConf) {
      this.contextManager = contextManager;
      this.spec = spec;
      this.localConf = contextualConf;
    }

    /**
     * To detect modifications to conf.
     * When an implementation modifies or wraps a conf, this should be the actual
     * object passed to the underlying implementation
     * @param conf the modified conf if not the original contextualConf
     */
    private void propagateConfigurationChanges(Configuration conf) {
      this.localConf = conf;
    }

    public <T> T newInstanceFromSpec(Class<T> parentClass) {
      return contextManager.newInstanceFromSpec(localConf, spec, parentClass);
    }

    public InputSplit newInstance(String className, Class<InputSplit> parentClass) {
      return contextManager.newInstance(localConf, className, parentClass);
    }

    public Configuration localConf() {
      return localConf;
    }

  }

  /**
   * A task to be executed in the context of a Spec (Library + conf)
   *
   * @author Julien Le Dem
   *
   * @param <T> the returned type
   */
  public static abstract class ContextualCall<T> {

    /**
     * will be called in the context of a conf and a Thread Context ClassLoader
     * @param localConf the conf resulting of the contextual overlay on the base conf
     * @return the result of the call
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract T call(CallContext context) throws IOException, InterruptedException;

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
      ClassDefinition inputFormatDefinition = lookup(classDefByName, spec.getClassDefinitionID());
      currentThread.setContextClassLoader(lookup(classLoaderByInputFormatName, spec.getClassDefinitionID()));
      Configuration before = newConf(globalConf, spec, inputFormatDefinition);
      CallContext context = new CallContext(this, spec, new Configuration(before));
      T result = callable.call(context);
      // detect changes in the conf and save them
      for (Entry<String, String> e : context.localConf) {
        String previous = before.getRaw(e.getKey());
        if (previous == null || !previous.equals(e.getValue())) {
          spec.getConf().put(e.getKey(), e.getValue());
        }
      }
      setSpecs(globalConf, asList(spec));
      return result;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("thread interrupted", e);
    } finally {
      currentThread.setContextClassLoader(contextClassLoader);
    }
  }

  private <T> T newInstanceFromSpec(Configuration contextualConf, Spec spec, Class<T> parentClass) {
    try {
      ClassDefinition classdef = lookup(classDefByName, spec.getClassDefinitionID());
      return newInstance(contextualConf, classdef.getClassName(), parentClass);
    } catch (RuntimeException e) {
      throw new RuntimeException("Can't instantiate class from spec " + spec, e);
    }
  }

  private <T> T newInstance(Configuration contextualConf, String className, Class<T> parentClass) {
    try {
      Class<?> clazz = contextualConf.getClassByName(className);
      return parentClass.cast(ReflectionUtils.newInstance(clazz, contextualConf));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

}
