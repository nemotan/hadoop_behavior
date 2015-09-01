package com.landray.behavior.job.hotspot;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.landray.behavior.db.MongoPool;
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
public class HotSpotPointJob extends AbstractHotSpotJob {
	//job创建时间，每次初始化的时候，该create重新赋值
	public static long create;
	public HotSpotPointJob(){
		create = System.currentTimeMillis();
	}
	public static final String JOB_NAME = "point";

	public String getJobName() {
		return JOB_NAME;
	}

	public Class getMapperClass() {
		return PointMapper.class;
	}

	public Class getReducerClass() {
		return PointReducer.class;
	}

	public Class<?> getJarClass() {
		return HotSpotPointJob.class;
	}

	public static class PointMapper extends HotSpotMapper {
		public void map(JSONObject env, String customerId, String fileDate, Context context)
				throws IOException, InterruptedException {
			String page = env.getString("i");
			String user = env.getString("u");
			
			// 热点点击
			JSONArray points = (JSONArray) env.get("p");
			if (points != null) {
				for (int j = 0; j < points.size(); j++) {
					JSONArray values = (JSONArray) points.get(j);
					//初始日志格式:
//{ "create" : 1437040705069 , "user" : "13db14c4d607b6077a18d3a45e7a8b15" , "page" : "p98284bf695b3a01d4dfb0a8f61a2ee64" , "key" : "-12,11" , "c" : 1 , "time" : 1430668800000}
					String key = values.getLong(0) + "," + values.getLong(1);
					long count = values.getLong(2);
					long time = 0l;
					// 该时间是file的时间
					try {
						time = DateUtil.convertDateStringToLong(fileDate);
					} catch (Exception e) {
						LOG.error("时间转换出错", e);
					}
					
				
					JSONObject keyJson = new JSONObject();
					keyJson.put("page", page);
					keyJson.put("key", key);
					keyJson.put("user", user);

					JSONObject valueJson = new JSONObject();
					valueJson.put("count", count);
					valueJson.put("time", create);//job的创建时间
					valueJson.put("beginTime", time);
					valueJson.put("endTime", time);
					
					// id以标记不同project
					context.write(new Text(customerId + ID_CONN + keyJson.toString()), new Text(valueJson.toString()));
					keyJson.put("user", "*");
					context.write(new Text(customerId + ID_CONN + keyJson.toString()), new Text(valueJson.toString()));
					//计算page（此处把原来的point job和page job合二为一）,並且移除多餘的属性
					keyJson.remove("key");
					keyJson.remove("user");
					valueJson.remove("count");
				    context.write(new Text(customerId + ID_CONN + keyJson.toString()), new Text(valueJson.toString()));
				}

			}
		}

	}

	public static class PointReducer extends HotSpotReduce {
		public String getCollectionName() {
			return JOB_NAME + "s";
		}
		
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
				String keyStr = key.toString().split(ID_CONN)[1];
				JSONObject keyJSON = JSONObject.fromObject(keyStr);
				if(keyJSON.containsKey("user")){
					//point
					  reducePoint(key,values,context);
				}else{
					//page 
					  reducePage(key,values,context);
				}
		}
		protected void reducePoint(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			long countR = 0l;
			long timeR = create;
			long beginTimeR = 0l;
			long endTimeR = 0l;
			
			for (Text value : values) {
				// 累加
				JSONObject valueJson = JSONObject.fromObject(value.toString());
				long count = Long.parseLong(valueJson.get("count").toString());
				long beginTime = Long.parseLong(valueJson.get("beginTime").toString());
				long endTime =  Long.parseLong(valueJson.get("endTime").toString());
				 
				countR += count;
				if(beginTimeR == 0 ||  beginTimeR > beginTime){
					beginTimeR = beginTime;
				}
				if(endTimeR == 0 ||  endTimeR < endTime){
					endTimeR = endTime;
				}
				
			}
			// 获取ID
			String id = key.toString().split(ID_CONN)[0];
			// 获取key
			String keyStr = key.toString().split(ID_CONN)[1];
			// 获取hotspot_res_id数据苦中的widgets集合
			DBCollection hotspotResConnection = getDBCollection(id);
			// point对象
			DBObject pointObject = new BasicDBObject();
			// _id对象
			DBObject keyObject = new BasicDBObject();
			keyObject = DBUtil.convertJson2DBObject(JSONObject.fromObject(keyStr));
			pointObject.put("_id", keyObject);
			// 如果数据苦存在该数据，累加并更新
			if (hotspotResConnection.count((pointObject)) > 0) {
				//获取已经存在的对象
				DBObject pointExistObject = hotspotResConnection.findOne(pointObject);
				//获取已经存在对象的alue
				DBObject pointExistValueObject = (DBObject) pointExistObject.get("value");
			  
				long count = Long.parseLong(pointExistValueObject.get("count").toString());
				long beginTime = Long.parseLong(pointExistValueObject.get("beginTime").toString());
				long endTime =  Long.parseLong(pointExistValueObject.get("endTime").toString());
				 
				countR += count;
				if(beginTimeR == 0 ||  beginTimeR > beginTime){
					beginTimeR = beginTime;
				}
				if(endTimeR == 0 ||  endTimeR < endTime){
					endTimeR = endTime;
				}
			}
			// value对象
			DBObject valueObject = new BasicDBObject();
			valueObject.put("count", countR);
			valueObject.put("time", timeR);
			valueObject.put("beginTime", beginTimeR);
			valueObject.put("endTime", endTimeR);
			
			pointObject.put("value", valueObject);
			hotspotResConnection.save(pointObject);
		   }
		
		
		protected void reducePage(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			long timeR = create;
			long beginTimeR = 0l;
			long endTimeR = 0l;
			
			for (Text value : values) {
				JSONObject valueJson = JSONObject.fromObject(value.toString());
				long beginTime = Long.parseLong(valueJson.get("beginTime").toString());
				long endTime =  Long.parseLong(valueJson.get("endTime").toString());
				 
				if(beginTimeR == 0 ||  beginTimeR > beginTime){
					beginTimeR = beginTime;
				}
				if(endTimeR == 0 ||  endTimeR < endTime){
					endTimeR = endTime;
				}
				
			}
			// 获取ID
			String id = key.toString().split(ID_CONN)[0];
			// 获取key
			String keyStr = key.toString().split(ID_CONN)[1];
			// 获取hotspot_res_id数据苦中的pages集合
			DBCollection hotspotResConnection = MongoPool.getInstance().getDB(JOB_RES + "_" + id)
					.getCollection("pages");
			// page对象
			DBObject pageObject = new BasicDBObject();
			// _id对象
			DBObject keyObject = new BasicDBObject();
			keyObject = DBUtil.convertJson2DBObject(JSONObject.fromObject(keyStr));
			pageObject.put("_id", keyObject);
			// 如果数据苦存在该数据，累加并更新
			if (hotspotResConnection.count((pageObject)) > 0) {
				//获取已经存在的对象
				DBObject pointExistObject = hotspotResConnection.findOne(pageObject);
				//获取已经存在对象的alue
				DBObject pointExistValueObject = (DBObject) pointExistObject.get("value");
			  
				long beginTime = Long.parseLong(pointExistValueObject.get("beginTime").toString());
				long endTime =  Long.parseLong(pointExistValueObject.get("endTime").toString());
				 
				if(beginTimeR == 0 ||  beginTimeR > beginTime){
					beginTimeR = beginTime;
				}
				if(endTimeR == 0 ||  endTimeR < endTime){
					endTimeR = endTime;
				}
			}
			DBObject valueObject = new BasicDBObject();
			valueObject.put("time", timeR);
			valueObject.put("beginTime", beginTimeR);
			valueObject.put("endTime", endTimeR);
			
			pageObject.put("value", valueObject);
			hotspotResConnection.save(pageObject);
		   }
	 }

}