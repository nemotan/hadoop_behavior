package com.landray.behavior.job;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.landray.behavior.db.MongoPool;
import com.landray.behavior.util.DBUtil;
import com.landray.behavior.util.DateUtil;
import com.landray.behavior.util.HDFSUtil;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HotSpotWidgetJob {
	protected static final Log LOG = LogFactory.getLog(HotSpotWidgetJob.class);
	public static final String JOB_CATE = "hotspot";
	public static final String JOB_RES = "res";
    public static final String JOB_RES_CONNECTION_WIDGETS = "widgets";
	public static final String JOB_NAME = "widget";

	public static class HotSpotMapper extends Mapper<Object, Text, Text, Text> {
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();// 读取一行
			if (line == null || line.equals("")) {
				return;
			}
			JSONObject lineJson = JSONObject.fromObject(line);
			String id = lineJson.get("id").toString();// 客户id
			String flleName = lineJson.getString("fileName").toString();// 文件名
			String fileDate = flleName.split("\\.")[2];// 文件时间
			String log = lineJson.getString("value");// 日志内容

			// log的格式为： data={t:Time, c:count, v:version, e:[{u:'user', i:'id',
			// p:[[x,y,count]], w:[[id, index, count]]}]}
			JSONObject json = JSONObject.fromObject(log);
			JSONArray envs = (JSONArray) json.get("e");
			if (envs == null) {
				return;
			}

			for (int i = 0; i < envs.size(); i++) {
				JSONObject env = envs.getJSONObject(i);
				String page = env.getString("i");

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
						String user = env.getString("u");
						String _key = "'" + values.getString(0) + "'," + values.getLong(1);
						long count = values.getLong(2);
						long time = 0l;
						// 该时间是file的时间
						try{
							 time =  DateUtil.convertDateStringToLong(fileDate) ;
						}catch(Exception e){
							LOG.error("时间转换出错",e);
						}
						/*
						 * emit({page:this.page, key:this.key, user:this.user},
						 * {count:this.c, time:$time$}); emit({page:this.page,
						 * key:this.key, user:'*'}, {count:this.c,
						 * time:$time$});
						 */
						JSONObject keyJson = new JSONObject();
						keyJson.put("page",page);
						keyJson.put("key", _key);
						keyJson.put("user",user);

						JSONObject valueJson = new JSONObject();
						valueJson.put("count", String.valueOf(count) );
						valueJson.put("time", String.valueOf(time));
						//id以标记不同project
						context.write(new Text(id + "-" + keyJson.toString()), new Text(valueJson.toString()));
						keyJson.put("user", "*");
						context.write(new Text(id + "-" + keyJson.toString()), new Text(valueJson.toString()));
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
				//累加
				JSONObject valueJson = JSONObject.fromObject(value.toString());
				int count = Integer.parseInt(valueJson.get("count").toString());
				time = Long.parseLong(valueJson.get("time").toString());
				sum += count;
			}
		   //获取ID
			String id = key.toString().split("-")[0];
			//获取key
			String keyStr = key.toString().split("-")[1];
			//获取hotspot_res_id数据苦中的widgets集合
			DBCollection hotspotResConnection = MongoPool.getInstance().getDB(JOB_CATE+"_"+JOB_RES+"_"+id)
					.getCollection(JOB_RES_CONNECTION_WIDGETS);
            //widget对象
			DBObject widgetObject = new BasicDBObject();
			//_id对象
			DBObject keyObject = new BasicDBObject();
			keyObject = DBUtil.convertJson2DBObject(JSONObject.fromObject(keyStr));
			widgetObject.put("_id",keyObject);
			//value对象
			DBObject valueObject = new BasicDBObject();
			valueObject.put("time", time);
		    //如果数据苦存在该数据，累加并更新	
			if(hotspotResConnection.count((widgetObject))>=0){
				DBObject widgetExistObject = hotspotResConnection.findOne(widgetObject);
			    int sumExist  =Integer.parseInt( ((DBObject)widgetExistObject.get("value")).get("count").toString());
			    sum += sumExist;
			}
			valueObject.put("count", sum );
			widgetObject.put("value", valueObject);
			hotspotResConnection.save(widgetObject);
		
			
			
			/*context.write((Text) key,
					new Text("{count:" + String.valueOf(sum) + ",time:" + String.valueOf(time) + "}"));*/
		}
	}

	public static void main(String[] args) throws Exception {
		String month = "2015-08";
		String date = "2015-08-07";
		if (args.length == 0) {
			args = new String[2];
			args[0] = "hdfs://localhost:9000/input/logs/" + month + "/" + JOB_CATE + "." + date + "*";
			args[1] = "hdfs://localhost:9000/output/logs/" + date + "/" + JOB_CATE+"_"+JOB_NAME ;
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, JOB_CATE+"_"+JOB_NAME + "_" + date);
		job.setJarByClass(HotSpotWidgetJob.class);
		job.setMapperClass(HotSpotMapper.class);

		job.setReducerClass(WidgetReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if (HDFSUtil.exits(conf, args[1])) {
			System.out.println(args[1] + "该路径已经存在,    先删除该目录......");
			System.out.println("删除结果:" + HDFSUtil.deleteFile(conf, args[1]));
		}
	    boolean result = job.waitForCompletion(true) ;
	    if(result){
	    		System.out.println("job："+job.getJobName()+"正常退出!");
	    }else{
	    		System.out.println("job："+job.getJobName()+"异常退出!");
	    }
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
