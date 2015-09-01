package com.nemo.examples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * 输入文本，以tab间隔
 * kaka    1       28
 * hua     0       26
 * chao    1
 * tao     1       22
 * mao     0       29      22
 * */

//Partitioner函数的使用

public class MyPartitioner {
	// Map函数
	public static class MyMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[] arr_value = value.toString().split("\t");
			//测试输出
//			for(int i=0;i<arr_value.length;i++)
//			{
//				System.out.print(arr_value[i]+"\t");
//			}
//			System.out.print(arr_value.length);
//			System.out.println();		
			Text word1 = new Text();
			Text word2 = new Text();
			if (arr_value.length > 3) {
				word1.set("long");
				word2.set(value);
			} else if (arr_value.length < 3) {
				word1.set("short");
				word2.set(value);
			} else {
				word1.set("right");
				word2.set(value);
			}
			output.collect(word1, word2);
		}
	}
	
	public static class MyReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			System.out.println(key);
			while (values.hasNext()) {
				output.collect(key, new Text(values.next().getBytes()));	
			}
		}
	}

	// 接口Partitioner继承JobConfigurable，所以这里有两个override方法
	public static class MyPartitionerPar implements Partitioner<Text, Text> {
		/**
		 * getPartition()方法的
		 * 输入参数：键/值对<key,value>与reducer数量numPartitions
		 * 输出参数：分配的Reducer编号，这里是result
		 * */
		public int getPartition(Text key, Text value, int numPartitions) {
			// TODO Auto-generated method stub
			int result = 0;
			System.out.println("numPartitions--" + numPartitions);
			if (key.toString().equals("long")) {
				result = 0 % numPartitions;
			} else if (key.toString().equals("short")) {
				result = 1 % numPartitions;
			} else if (key.toString().equals("right")) {
				result = 2 % numPartitions;
			}
			System.out.println("result--" + result);
			return result;
		}
		
		public void configure(JobConf arg0)
		{
			// TODO Auto-generated method stub
		}
	}

	//输入参数：/home/hadoop/input/PartitionerExample /home/hadoop/output/Partitioner
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(MyPartitioner.class);
		conf.setJobName("MyPartitioner");
		
		//控制reducer数量，因为要分3个区，所以这里设定了3个reducer
		conf.setNumReduceTasks(3);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		//设定分区类
		conf.setPartitionerClass(MyPartitionerPar.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		//设定mapper和reducer类
		conf.setMapperClass(MyMap.class);
		conf.setReducerClass(MyReduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
