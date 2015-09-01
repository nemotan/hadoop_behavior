package com.landray.behavior.job.hotspot;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.landray.behavior.util.DBUtil;
import com.landray.behavior.util.DateUtil;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * 
 * 
 * @author nemo
 *
 */
public class HotSpotWidgetJob extends AbstractHotSpotJob {
	public static final String JOB_NAME = "widget";

	public String getJobName() {
		return JOB_NAME;
	}

	public Class getMapperClass() {
		return WidgetMapper.class;
	}

	public Class getReducerClass() {
		return WidgetReducer.class;
	}

	public Class<?> getJarClass() {
		return HotSpotWidgetJob.class;
	}

	public static class WidgetMapper extends HotSpotMapper {
		public void map(JSONObject env, String customerId, String fileDate, Context context)
				throws IOException, InterruptedException {
			String page = env.getString("i");
			String user = env.getString("u");

			// 部件点击
			JSONArray widgets = (JSONArray) env.get("w");
			if (widgets != null) {
				for (int j = 0; j < widgets.size(); j++) {
					JSONArray values = (JSONArray) widgets.get(j);
					/**
					 * { "_id" : ObjectId("55a78041c2c8f167cc3b0015"), "create"
					 * : NumberLong("1437040613741"), "user" :
					 * "1271da589e889cf60a05da64837aca53", "page" :
					 * "p4cf42a5df5b9ec6a44afb6ac7a4dadac", "key" :
					 * "'p_51ad719cea4b2d268f0b',0", "c" : NumberLong(3), "time"
					 * : NumberLong("1430668800000") }
					 * 
					 */
					String key = "'" + values.getString(0) + "',"
							+ values.getLong(1);
					long count = values.getLong(2);
					long time = 0l;
					// 该时间是file的时间
					try {
						time = DateUtil.convertDateStringToLong(fileDate);
					} catch (Exception e) {
						LOG.error("时间转换出错", e);
					}
					/*
					 * emit({page:this.page, key:this.key, user:this.user},
					 * {count:this.c, time:$time$}); emit({page:this.page,
					 * key:this.key, user:'*'}, {count:this.c, time:$time$});
					 */
					JSONObject keyJson = new JSONObject();
					keyJson.put("page", page);
					keyJson.put("key", key);
					keyJson.put("user", user);

					JSONObject valueJson = new JSONObject();
					valueJson.put("count", count);
					valueJson.put("time",time);
					// id以标记不同project
					context.write(new Text(customerId + ID_CONN + keyJson.toString()), new Text(valueJson.toString()));
					keyJson.put("user", "*");
					context.write(new Text(customerId + ID_CONN + keyJson.toString()), new Text(valueJson.toString()));
				}

			}
		}

	}

	public static class WidgetReducer extends HotSpotReduce {
		public String getCollectionName() {
			return JOB_NAME + "s";
		}

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			long time = 0l;
			for (Text value : values) {
				// 累加
				JSONObject valueJson = JSONObject.fromObject(value.toString());
				int count = Integer.parseInt(valueJson.get("count").toString());
				time = Long.parseLong(valueJson.get("time").toString());
				sum += count;
			}
			// 获取ID
			String id = key.toString().split(ID_CONN)[0];
			// 获取key
			String keyStr = key.toString().split(ID_CONN)[1];
			// 获取hotspot_res_id数据苦中的widgets集合
			DBCollection hotspotResConnection = getDBCollection(id);
			// widget对象
			DBObject widgetObject = new BasicDBObject();
			// _id对象
			DBObject keyObject = new BasicDBObject();
			keyObject = DBUtil.convertJson2DBObject(JSONObject.fromObject(keyStr));
			widgetObject.put("_id", keyObject);
			// value对象
			DBObject valueObject = new BasicDBObject();
			valueObject.put("time", time);
			// 如果数据苦存在该数据，累加并更新
			if (hotspotResConnection.count((widgetObject)) > 0) {
				DBObject widgetExistObject = hotspotResConnection.findOne(widgetObject);
				int sumExist = Integer.parseInt(((DBObject) widgetExistObject.get("value")).get("count").toString());
				sum += sumExist;
			}
			valueObject.put("count", sum);
			widgetObject.put("value", valueObject);
			hotspotResConnection.save(widgetObject);

			/*
			 * context.write((Text) key, new Text("{count:" +
			 * String.valueOf(sum) + ",time:" + String.valueOf(time) + "}"));
			 */
		}
	}

}