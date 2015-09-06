package com.landray.behavior.job.hotspot;

import com.landray.behavior.job.base.BehaviorJob;
import com.landray.behavior.job.base.JobConst;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * hotspot基础抽象类
 * 
 * @author nemo
 *
 */
public abstract class AbstractHotSpotJob extends BehaviorJob{
	public String getJobCate() {
		return JobConst.JOB_CATE_HOTSPOT;
	}

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
	}
}
