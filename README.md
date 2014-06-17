An Isolated InputFormat for Hadoop.
===================

This InputFormat allows decoupling the format of the data from the actual jobs. It implements isolation from the classloader and configuration perspective.
It is intended as a better MultiInputFormat which allows reading a single input from multiple locations with different inputformats coming from separate isolated class loaders (child first class loading delegation) and configurations.

Configuration
-------------

3 types of entities need to be configured. They are all identified by a unique name/id.

### Libraries

a library is defined by a list of jars on HDFS.
Each library will create a classloader.
```
com.twitter.isolated.library.{library id}.paths={comma delimited list of jar paths on HDFS}
```
example:
```
com.twitter.isolated.library.parquet-lib.paths=hdfs:///libs/parquet-hadoop-bundle-1.4.3.jar
```

### Class definitions
a Class is defined by a class name and (optionally) a library name and conf
```
com.twitter.isolated.class.{class definition id}.library={library ID}
com.twitter.isolated.class.{class definition id}.name={class name}
com.twitter.isolated.class.{class definition id}.conf.{key}={value}
```
if library is not defined, the current application class loader is used. Otherwise the library class loader is used.

examples:
```
com.twitter.isolated.class.parquet-inputformat.library=parquet-lib
com.twitter.isolated.class.parquet-inputformat.name=parquet.hadoop.ParquetInputFormat
com.twitter.isolated.class.parquet-inputformat.conf.parquet.read.support.class=parquet.hadoop.example.GroupReadSupport

com.twitter.isolated.class.text-inputformat.name=org.apache.hadoop.mapreduce.lib.input.TextInputFormat
```

### Specs
a Spec is defined by a class definition and a conf.

```
com.twitter.isolated.spec.{spec id}.class={class definition id}
com.twitter.isolated.spec.{spec id}.conf.{key}={value}
```
example:
```
com.twitter.isolated.spec.0.class=parquet-inputformat
com.twitter.isolated.spec.0.conf.mapred.input.dir=/user/julien/myfile.parquet

com.twitter.isolated.spec.1.class=text-inputformat
com.twitter.isolated.spec.1.conf.mapred.input.dir=/user/julien/job_stats_by_day

com.twitter.isolated.spec.o.class=text-outputformat
com.twitter.isolated.spec.o.conf.mapred.output.dir=/user/julien/out
```

# Input
The specs defining the input.
```
com.twitter.isolated.inputspecs={comma delimoted list of specs}
```
example:
```
com.twitter.isolated.inputspecs=0,1
```

# Output
The spec defining the output.
```
com.twitter.isolated.output={spec id}
```
example:
```
com.twitter.isolated.outputspec=o
```


