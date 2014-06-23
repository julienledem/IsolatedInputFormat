package com.twitter.isolated.hadoop.mapred;

import static com.twitter.isolated.hadoop.IsolatedConf.setClassDefinitions;
import static com.twitter.isolated.hadoop.IsolatedConf.setInputSpecs;
import static com.twitter.isolated.hadoop.IsolatedConf.setLibraries;
import static com.twitter.isolated.hadoop.IsolatedConf.setOutputSpec;
import static com.twitter.isolated.hadoop.IsolatedConf.setSpecs;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.isolated.hadoop.ClassDefinition;
import com.twitter.isolated.hadoop.Library;
import com.twitter.isolated.hadoop.Spec;

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



  @Test
  public void testClassLoaderIsolation() throws Exception {
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
    Path parquetJar = fileSystem.makeQualified(new Path("/Users/julien/parquet-hadoop-bundle.jar"));
    IOUtils.copyBytes(
        new URL("http://repo1.maven.org/maven2/com/twitter/parquet-hadoop-bundle/1.4.3/parquet-hadoop-bundle-1.4.3.jar").openStream(),
        fileSystem.create(parquetJar),
        conf);

    // configure job
    Job job = new Job(mrCluster.createJobConf());
    JobConf jobConf = job.getJobConf();
    setLibraries(
        jobConf,
        asList(
            new Library("parquet-lib", parquetJar)
            )
        );
    setClassDefinitions(
        jobConf,
        asList(
            new ClassDefinition("parquet-inputformat", "parquet-lib", "parquet.hadoop.mapred.DeprecatedParquetInputFormat", "parquet.read.support.class=parquet.hadoop.example.GroupReadSupport"),
            new ClassDefinition("text-inputformat", null, TextInputFormat.class.getName()),
            new ClassDefinition("text-outputformat", null, TextOutputFormat.class.getName())
            )
        );
    setSpecs(
        jobConf,
        asList(
            new Spec("0", "parquet-inputformat", "mapred.input.dir=" + in.toUri()),
            new Spec("1", "text-inputformat", "mapred.input.dir=" + in2.toUri()),
            new Spec("output", "text-outputformat", "mapred.output.dir=" + out.toUri())
            )
        );
    setInputSpecs(jobConf, "0", "1");
    setOutputSpec(jobConf, "output");

    jobConf.setInputFormat(IsolatedInputFormat.class);
    jobConf.setNumReduceTasks(0);
    jobConf.setOutputFormat(IsolatedOutputFormat.class);

    RunningJob runningJob = job.getJobClient().submitJob(jobConf);
    waitForJob(runningJob);
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

  private void validate(InputStream s, String v) throws IOException {
    BufferedReader r = new BufferedReader(new InputStreamReader(s));
    assertEquals("key:" + v + "\tvalue:" + v, r.readLine());
    r.close();
  }

  private void waitForJob(RunningJob runningJob) throws InterruptedException, IOException {

    while (!runningJob.isComplete()) {
      System.out.println("waiting for job " + runningJob.getJobName() + " " + (int)(runningJob.mapProgress() * 100) + "%");
      sleep(100);
    }
    System.out.println("status for job " + runningJob.getJobName() + ": " + (runningJob.isSuccessful() ? "SUCCESS" : "FAILURE"));
    if (!runningJob.isSuccessful()) {
      throw new RuntimeException("job failed " + runningJob.getJobName());
    }
  }
}
