package com.nemo.test;

import com.landray.behavior.base.util.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.StringTokenizer;

public class Main {
	public static final String INPUT = "hdfs://master:9000/input/wordcount";
	public static final String OUT_PUT = "hdfs://master:9000/output/wordcount";
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineArr = line.split(" ");
			for(String s: lineArr) {
				word.set(s);
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
						   Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	
	Configuration conf = new Configuration();
	conf.set("yarn.resourcemanager.address", "master"+":"+8032); //设置RM 访问位置
	conf.set("mapreduce.jobhistory.address", "master:10020");
	Job job = Job.getInstance(conf, "word count");
	job.setJar("/Users/nemo/03ws/sp_intel/out/artifacts/hadoop_behavior_jar/hadoop_behavior.jar");
	//job.setMapperClass(TokenizerMapper.class);
	FileInputFormat.addInputPath(job, new Path(INPUT));
	FileOutputFormat.setOutputPath(job, new Path(OUT_PUT));
	if (HDFSUtil.exits(conf, OUT_PUT)) {
		System.out.println("改路径已经存在,先删除该目录......");
		System.out.println("删除结果:" + HDFSUtil.deleteFile(conf, OUT_PUT));
	}
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
