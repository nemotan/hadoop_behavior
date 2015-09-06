package com.nemo.test;

import com.landray.behavior.base.util.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main {
	public static final String INPUT = "hdfs://master:9000/input/wordcount";
	public static final String OUT_PUT = "hdfs://master:9000/output/wordcount";
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	
	Configuration conf = new Configuration();
	conf.set("yarn.resourcemanager.address", "192.168.5.146"+":"+8032); //设置RM 访问位置
	conf.set("mapreduce.jobhistory.address","192.168.5.146:10020");
	Job job = Job.getInstance(conf, "word count");
	job.setJar("/Users/nemo/03ws/sp_hadoop/hadoop_behavior/hadoop_behavior/src/main/java/behavior.jar");
	FileInputFormat.addInputPath(job, new Path(INPUT));
	FileOutputFormat.setOutputPath(job, new Path(OUT_PUT));
	if (HDFSUtil.exits(conf, OUT_PUT)) {
		System.out.println("改路径已经存在,先删除该目录......");
		System.out.println("删除结果:" + HDFSUtil.deleteFile(conf, OUT_PUT));
	}
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
