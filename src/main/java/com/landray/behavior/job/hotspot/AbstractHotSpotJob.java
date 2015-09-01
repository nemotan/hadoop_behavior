package com.landray.behavior.job.hotspot;

import com.mongodb.DBCollection;
import com.landray.behavior.db.MongoPool;
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

/**
 * hotspot基础抽象类
 * 
 * @author nemo
 *
 */
public abstract class AbstractHotSpotJob {
	protected static final Log LOG = LogFactory.getLog(AbstractHotSpotJob.class);
	public static final String JOB_CATE = "hotspot";// job类型
	public static final String JOB_RES = JOB_CATE + "_res";// job结果数据库
    public static final String ID_CONN = "----";
	/**
	 * 获取下一个job
	 * 
	 * @return
	 */
	public AbstractHotSpotJob getNextJob() {
		return null;
	}
	/**
	 * 根据日期提交当天的job
	 * 
	 * @param date
	 * @return
	 * @throws Exception
	 */
	public boolean run(String date) throws Exception {
		Job job = getJob(date);
		boolean result = job.waitForCompletion(true);
		if (result) {
			LOG.info("job：" + job.getJobName() + "正常退出!");
			//提交下一个job
			if(getNextJob() != null){
				  getNextJob().run(date);
			}
		} else {
			LOG.info("job：" + job.getJobName() + "异常退出!");
		}
		return result;
	}

	/**
	 * 获取hadoop job
	 * 
	 * @return
	 * @throws Exception
	 */
	public Job getJob(String date) throws Exception {
		// String month = "2015-08";
		String month = date.substring(0, date.lastIndexOf("-"));
		String inputPath = "hdfs://localhost:9000/input/logs/" + month + "/" + JOB_CATE + "." + date + "*";
		String outPath = "hdfs://localhost:9000/output/logs/" + date + "/" + JOB_CATE + "_" + getJobName();

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, JOB_CATE + "_" + getJobName() + "_" + date);
		job.setJarByClass(getJarClass());
		job.setMapperClass(getMapperClass());

		job.setReducerClass(getReducerClass());
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		if (HDFSUtil.exits(conf, outPath)) {
			System.out.println(outPath + "该路径已经存在,    先删除该目录......");
			System.out.println("删除结果:" + HDFSUtil.deleteFile(conf, outPath));
		}
		return job;
	}

	/**
	 * 获取job名 for: widget|point|page
	 * 
	 * @return
	 */
	public abstract String getJobName();

	/**
	 * 获取job的mapper class
	 * 
	 * @return
	 */
	public abstract Class<? extends Mapper> getMapperClass();

	/**
	 * 获取job的reducer class
	 * 
	 * @return
	 */
	public abstract Class<? extends Reducer> getReducerClass();
	/**
	 * 
	 * 获取jar class
	 * @return
	 */
	public abstract Class<?> getJarClass();

	public abstract static class HotSpotMapper extends Mapper<Object, Text, Text, Text> {
		/**
		 * 明细日志map方法
		 * 
		 * @param env
		 * @param customerId
		 * @param fileDate
		 * @param context
		 * @throws Exception
		 */
		public abstract void map(JSONObject env, String customerId, String fileDate, Context context)
				throws IOException, InterruptedException;

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
				map(env, id, fileDate, context);
			}
		}
	}

	public abstract static class HotSpotReduce extends Reducer<Text, Text, Text, Text> {
		/**
		 * 获取结果存放的mongodb collection name
		 * 
		 * @return
		 */
		public abstract String getCollectionName();

		/**
		 * 根据客ID获取存储结果的collection 如：hotsopt_res_123456数据库下的：widgets集合
		 * 
		 * @param customerId
		 * @return
		 */
		public DBCollection getDBCollection(String customerId) {
			DBCollection hotspotResConnection = MongoPool.getInstance().getDB(JOB_RES + "_" + customerId)
					.getCollection(getCollectionName());
			return hotspotResConnection;
		}
	}
}
