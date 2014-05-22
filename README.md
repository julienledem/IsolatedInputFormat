An Isolated InputFormat for Hadoop.
===================

This InputFormat allows decoupling the format of the data from the actual jobs. It implement isolation from classloader and configuration perspective.
It is intended as a better MultiInputFormat which allows reading a single input from multiple locations with different inputformats comming from separate isolated class loaders (child first class loading delegation) and configurations.

Configuration
-------------

3 types of entities need to be configured. They are all identified by a unique name/id.

### Libraries

a library is defined by a list of jars on HDFS.
Each library will create a classloader.
```
com.twitter.isolated.library.{library name}.paths={space delimited list of jar paths on HDFS}
```
example:
```
com.twitter.isolated.library.parquet-lib.paths=hdfs:///libs/parquet-hadoop-bundle-1.4.3.jar
```

### InputFormats
an InputFormat is defined by an InputFormat class name and (optionally) a library name and conf
```
com.twitter.isolated.inputformat.{name}.library={library name}
com.twitter.isolated.inputformat.{name}.class={class name}
com.twitter.isolated.inputformat.{name}.conf.{key}={value}
```
if library is not defined, the current application class loader is used. Otherwise the library class loader is used.

examples:
```
com.twitter.isolated.inputformat.parquet-inputformat.library=parquet-lib
com.twitter.isolated.inputformat.parquet-inputformat.class=parquet.hadoop.ParquetInputFormat
com.twitter.isolated.inputformat.parquet-inputformat.conf.parquet.read.support.class=parquet.hadoop.example.GroupReadSupport

com.twitter.isolated.inputformat.text-inputformat.class=org.apache.hadoop.mapreduce.lib.input.TextInputFormat
```

### InputSpecs
an InputSpec is defined by an inputformat and a conf.

```
com.twitter.isolated.inputspec.{id}.inputformat={input format name}
com.twitter.isolated.inputspec.{id}.conf.{key}={value}
```
example:
```
com.twitter.isolated.inputspec.0.inputformat=parquet-inputformat
com.twitter.isolated.inputspec.0.conf.mapred.input.dir=/user/julien/client_event_2013_12_19_15.parquet

com.twitter.isolated.inputspec.1.inputformat=text-inputformat
com.twitter.isolated.inputspec.1.conf.mapred.input.dir=/user/julien/job_stats_by_day
mapred.output.dir=example/out
```
