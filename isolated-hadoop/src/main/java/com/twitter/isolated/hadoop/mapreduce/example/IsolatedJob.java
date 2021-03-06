package com.twitter.isolated.hadoop.mapreduce.example;

import static java.lang.Thread.sleep;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.twitter.isolated.hadoop.mapreduce.IsolatedInputFormat;

public class IsolatedJob {

  public static void main(String[] args) throws Exception {
    Properties properties = new Properties();
    properties.load(new FileInputStream(new File(args[0])));
    Configuration conf = new Configuration();


    Job job = new Job(conf, "example");
    for (Entry<Object, Object> entry : properties.entrySet()) {
      job.getConfiguration().set((String)entry.getKey(), (String)entry.getValue());
    }

    job.setInputFormatClass(IsolatedInputFormat.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapperClass(Mapper.class);
    job.setJarByClass(IsolatedJob.class);
    job.submit();
    System.out.println("job id = " + job.getJobID());
    System.out.println("URL: " + job.getTrackingURL());
    waitForJob(job);
  }


  private static void waitForJob(Job job) throws InterruptedException, IOException {
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
