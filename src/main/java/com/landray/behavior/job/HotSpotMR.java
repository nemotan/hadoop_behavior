package com.landray.behavior.job;

import com.landray.behavior.util.HDFSUtil;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HotSpotMR {
	public final static String INPUT_PATH = "hdfs://192.168.5.113:9000/logs/123456";
	public final static String OUTPUT_PATH = "hdfs://192.168.5.113:9000/output/logs";

	public static class HotSpotMapper extends Mapper<Object, Text, Text, Text> {
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String log = value.toString();
			// data={t:Time, c:count, v:version, e:[{u:'user', i:'id',
			// p:[[x,y,count]], w:[[id, index, count]]}]}
			JSONObject json = JSONObject.fromObject(log);
			JSONArray envs = (JSONArray) json.get("e");
			if (envs == null) {
				return;
			}
			for (int i = 0; i < envs.size(); i++) {
				JSONObject env = envs.getJSONObject(i);
				String page = env.getString("i");
				// 热点点击
				JSONArray points = (JSONArray) env.get("p");
				if (points != null) {
					for (int j = 0; j < points.size(); j++) {
						/*
						 * JSONArray values = (JSONArray) points.get(j); String
						 * key = values.getLong(0) + "," + values.getLong(1);
						 * long count = values.getLong(2); BasicDBObject doc =
						 * buildDoc(env).append("page", page) .append("key",
						 * key).append("c", count).append( "time", time);
						 * pointColl.insert(doc);
						 */
					}
				}
				// 部件点击
				JSONArray widgets = (JSONArray) env.get("w");
				if (widgets != null) {
					for (int j = 0; j < widgets.size(); j++) {
						JSONArray values = (JSONArray) widgets.get(j);
						/**
						 * { "_id" : ObjectId("55a78041c2c8f167cc3b0015"),
						 * "create" : NumberLong("1437040613741"), "user" :
						 * "1271da589e889cf60a05da64837aca53", "page" :
						 * "p4cf42a5df5b9ec6a44afb6ac7a4dadac", "key" :
						 * "'p_51ad719cea4b2d268f0b',0", "c" : NumberLong(3),
						 * "time" : NumberLong("1430668800000") }
						 * 
						 */
						long create = System.currentTimeMillis();
						String user = env.getString("u");
						String _page = page;
						String _key = "'" + values.getString(0) + "',"
								+ values.getLong(1);
						long count = values.getLong(2);
						long time = create; // TODO 该时间是file的时间
						/*
						 * emit({page:this.page, key:this.key, user:this.user},
						 * {count:this.c, time:$time$}); emit({page:this.page,
						 * key:this.key, user:'*'}, {count:this.c,
						 * time:$time$});
						 */
						context.write(
								new Text("{page:" + page + ",key:" + _key
										+ ",user:" + user + "}"),
								new Text("{count:" + String.valueOf(count)
										+ ",time:" + String.valueOf(time) + "}"));
						context.write(
								new Text("{page:" + page + ",key:" + _key
										+ ",user:'*'}"),
								new Text("{count:" + String.valueOf(count)
										+ ",time:" + String.valueOf(time) + "}"));
					}
				}
			}
		}
	}

	public static class WidgetReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			long time = 0l;
			for (Text value : values) {
				JSONObject valueJson = JSONObject.fromObject(value.toString());
				int count = Integer.parseInt(valueJson.get("count").toString());
				// TODO time需要更新数据库中的时间
				time = Long.parseLong(valueJson.get("time").toString());
				sum += count;
			}
			/* "{count:0, time:1437111558154};"; */
			context.write((Text) key, new Text("{count:" + String.valueOf(sum)
					+ ",time:" + String.valueOf(time) + "}"));
		}
	}

	public static class PointReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write((Text) key, (Text) value);
			}
		}
	}

	public static class PageReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write((Text) key, (Text) value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(HotSpotMR.class);
		job.setMapperClass(HotSpotMapper.class);
		job.setReducerClass(WidgetReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		if (HDFSUtil.exits(conf, OUTPUT_PATH)) {
			System.out.println(OUTPUT_PATH+"该路径已经存在,    先删除该目录......");
			System.out.println("删除结果:" + HDFSUtil.deleteFile(conf, OUTPUT_PATH));
		}
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
