package com.landray.behavior.job.hotspot;

import com.landray.behavior.base.db.DBNames;
import com.landray.behavior.job.base.JobConst;
import com.landray.behavior.base.util.DBUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import net.sf.json.JSONObject;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by nemo on 15-9-2.
 */
public class HotSpotWidgetOutputFormat<K, V> extends FileOutputFormat<K, V> {
    public static final String COLLECTION_NAME_WIDGET = "widgets";

    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new HotSpotWidgetOutputFormat.LineRecordWriter();
    }
    protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> {
        public LineRecordWriter() {
        }

        public synchronized void write(K key, V value) throws IOException {
            //get the time and sum
            JSONObject vJSON = JSONObject.fromObject(value.toString());
            int sum = Integer.parseInt(vJSON.get("sum").toString());
            long time = Long.parseLong(vJSON.get("time").toString());
            // 获取ID
            String id = key.toString().split(JobConst.ID_CONN)[0];
            // 获取key
            String keyStr = key.toString().split(JobConst.ID_CONN)[1];
            // 获取hotspot_res_id数据苦中的widgets集合
            DBCollection hotspotResConnection = DBUtil.getDBCollection(DBNames.COLLECTION_RES_HOTSPOT+ "_" +id, COLLECTION_NAME_WIDGET);
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
        }
        public synchronized void close(TaskAttemptContext context) throws IOException {
        }
    }
}
