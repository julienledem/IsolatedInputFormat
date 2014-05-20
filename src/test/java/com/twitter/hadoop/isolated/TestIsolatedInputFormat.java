package com.twitter.hadoop.isolated;

import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestIsolatedInputFormat {
  private MiniDFSCluster dfsCluster;
  private MiniMRCluster mrCluster;
  private Configuration conf;

  @Before
  public void setup() {
    conf = new Configuration();
    conf.set("mapred.map.max.attempts", "1");
    conf.set("mapred.reduce.max.attempts", "1");
    try {
      System.setProperty("hadoop.log.dir", "/tmp/logs");
      dfsCluster = new MiniDFSCluster(conf, 1, true, null);
      mrCluster = new MiniMRCluster(1, getFileSystem().getUri().toString(), 1);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @After
  public void tearDown() {
    mrCluster.shutdown();
    dfsCluster.shutdown();
  }

  private FileSystem getFileSystem() throws IOException {
    return dfsCluster.getFileSystem();
  }

  public static class MyMapper extends Mapper<String, String, String, String> {
  }

  @Test
  public void test() throws Exception {
    FileSystem fileSystem = getFileSystem();
    final Path in = new Path("target/testData/TestIsolatedInputFormat/in/parquet");
    final Path in2 = new Path("target/testData/TestIsolatedInputFormat/in/text");
    Path out = new Path("target/testData/TestIsolatedInputFormat/out");
    fileSystem.delete(in, true);
    fileSystem.delete(in2, true);
    fileSystem.delete(out, true);
    // create Parquet input
    FSDataOutputStream input = fileSystem.create(in);
    IOUtils.copyBytes(this.getClass().getClassLoader().getResourceAsStream("part-m-00000.parquet"), input, conf);
    // create text input
    FSDataOutputStream input2 = fileSystem.create(in2);
    input2.write("Foo".getBytes());
    input2.close();

    // put parquet jar on HDFS
    Path parquetJar = fileSystem.makeQualified(new Path("/Users/julien/parquet-hadoop-bundle-1.4.2-SNAPSHOT.jar"));
    IOUtils.copyBytes(
        new FileInputStream("/Users/julien/.m2/repository/com/twitter/parquet-hadoop-bundle/1.4.2-SNAPSHOT/parquet-hadoop-bundle-1.4.2-SNAPSHOT.jar"),
        fileSystem.create(parquetJar),
        conf);

    // configure job
    Job job = new Job(mrCluster.createJobConf());
    IsolatedInputFormat.setLibraries(
        job,
        asList(
            new Library("parquet-lib", parquetJar)
            )
        );
    IsolatedInputFormat.setInputFormats(
        job,
        asList(
            new InputFormatDefinition("parquet-inputformat", "parquet-lib", "parquet.hadoop.ParquetInputFormat", "parquet.read.support.class=parquet.hadoop.example.GroupReadSupport"),
            new InputFormatDefinition("text-inputformat", null, "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")
            )
        );
    IsolatedInputFormat.setInputSpecs(
        job,
        asList(
            new InputSpec("parquet-inputformat", "mapred.input.dir=" + in.toUri()),
            new InputSpec("text-inputformat", "mapred.input.dir=" + in2.toUri())
            )
        );

    job.setInputFormatClass(IsolatedInputFormat.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapperClass(MyMapper.class);
    TextOutputFormat.setOutputPath(job, out);

    for (Entry<String, String> e : job.getConfiguration()) {
      System.out.println(e);
    }
    job.submit();
    waitForJob(job);
    FileStatus[] list = fileSystem.listStatus(out);
    for (FileStatus fileStatus : list) {
      System.out.println(fileStatus.getPath());
    }
    for (FileStatus fileStatus : list) {
      if (!fileStatus.getPath().getName().startsWith("_")) {
        System.out.println(fileStatus.getPath());
        FSDataInputStream o = fileSystem.open(fileStatus.getPath());
        IOUtils.copyBytes(o, System.out, 64000, false);
        o.close();
      }
    }
  }

  private void waitForJob(Job job) throws InterruptedException, IOException {
    while (!job.isComplete()) {
      System.out.println("waiting for job " + job.getJobName() + " " + (int)(job.mapProgress() * 100) + "%");
      sleep(100);
    }
    System.out.println("status for job " + job.getJobName() + ": " + (job.isSuccessful() ? "SUCCESS" : "FAILURE"));
    if (!job.isSuccessful()) {
      throw new RuntimeException("job failed " + job.getJobName());
    }
  }
}
