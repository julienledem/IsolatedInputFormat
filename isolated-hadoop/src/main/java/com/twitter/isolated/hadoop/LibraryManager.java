package com.twitter.isolated.hadoop;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class LibraryManager {

  private static final byte[] ZIP_FILE_HEADER = { 80, 75, 3, 4 };

  private static URL[] toURLs(List<Path> jars) {
    try {
      URL[] result = new URL[jars.size()];
      for (int i = 0; i < result.length; i++) {
        Path path = jars.get(i);
        Configuration conf = new Configuration();
        FileSystem fs = path.getFileSystem(conf);
        validate(path, fs);
        URI uri = path.toUri();
        if (!uri.getScheme().equals("file")) {
          // TODO use some kind of jar cache mechanism
          File tmp = File.createTempFile(path.getName(), ".jar");
          tmp.deleteOnExit();
          FSDataInputStream s = fs.open(path);
          FileOutputStream fso = new FileOutputStream(tmp);
          try {
            IOUtils.copyBytes(s, fso, conf);
          } finally {
            IOUtils.closeStream(fso);
            IOUtils.closeStream(s);
          }
          result[i] = tmp.toURI().toURL();
        } else {
          throw new RuntimeException("jars should be on HDFS: " + path);
//          result[i] = uri.toURL();
        }
      }
      return result;
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void validate(Path path, FileSystem fs) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(path);
    if (fileStatus.isDir()) {
      throw new RuntimeException(path + " should be a jar");
    }
    byte[] header = new byte[4];
    FSDataInputStream s = fs.open(path);
    s.readFully(header);
    s.close();
    if (!Arrays.equals(ZIP_FILE_HEADER, header)) {
      throw new RuntimeException("path " + path + " is not a valid jar");
    }
  }

  private static final class IsolatedClassLoader extends URLClassLoader {
    private IsolatedClassLoader(Library lib, ClassLoader parent) {
      super(toURLs(lib.getJars()), parent);
    }

    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve)
        throws ClassNotFoundException {
      Class<?> c = findLoadedClass(name);
      if (c == null && !apiClass(name)) {
        try {
          c = findClass(name);
        } catch (ClassNotFoundException e) { }
      }
      if (c == null) { // try parent
        c = getParent().loadClass(name);
      }
      if (resolve) {
        resolveClass(c);
      }
      return c;
    }

    /**
     * Will prevent loading in the Library classes that should only be loaded in
     * the parent classloader
     * @param name the class name
     * @return true if it should not be loaded in this classLoader
     */
    private boolean apiClass(String name) {
      // this class verifies that it is loaded only once.
      // it will fail otherwise
      return name.equals("org.apache.commons.logging.Log");
    }
  }

  private static Map<Library, ClassLoader> classLoaderByLib = new HashMap<Library, ClassLoader>();

  /**
   * ensures we always return the same class loader for the same library definition
   * otherwise you can get "Foo can not be cast to Foo" errors
   * @param lib the lib definition
   * @return the corresponding classloader
   */
  public static ClassLoader getClassLoader(Library lib) {
    ClassLoader classLoader = classLoaderByLib.get(lib);
    if (classLoader == null) {
      ClassLoader parent = LibraryManager.class.getClassLoader();
      classLoader = lib == null ? parent : new IsolatedClassLoader(lib, parent);
      classLoaderByLib.put(lib, classLoader);
    }
    return classLoader;
  }

}
