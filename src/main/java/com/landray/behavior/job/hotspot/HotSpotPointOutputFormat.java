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
public class HotSpotPointOutputFormat<K, V> extends FileOutputFormat<K, V> {
    public static final String COLLECTION_NAME_POINT = "points";
    public static final String COLLECTION_NAME_PAGE = "pages";

    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new HotSpotPointOutputFormat.LineRecordWriter();
    }

    protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> {
        public LineRecordWriter() {
        }

        public synchronized void write(K key, V value) throws IOException {
            String keyStr = key.toString().split(JobConst.ID_CONN)[1];
            JSONObject keyJSON = JSONObject.fromObject(keyStr);
            if (keyJSON.containsKey("user")) {
                //point
                writePoint(key, value);
            } else {
                //page
                writePage(key, value);
            }

        }

        private void writePoint(K key, V value) {
            //get the time and sum
            JSONObject vJSON = JSONObject.fromObject(value.toString());
            long countR = Long.parseLong(vJSON.get("countR").toString());
            long beginTimeR = Long.parseLong(vJSON.get("beginTimeR").toString());
            long endTimeR = Long.parseLong(vJSON.get("endTimeR").toString());

            // 获取ID
            String id = key.toString().split(JobConst.ID_CONN)[0];
            // 获取key
            String keyStr = key.toString().split(JobConst.ID_CONN)[1];
            // 获取hotspot_res_id数据苦中的points集合
            DBCollection hotspotResConnection = DBUtil.getDBCollection(DBNames.COLLECTION_RES_HOTSPOT + "_" + id, COLLECTION_NAME_POINT);
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
                long endTime = Long.parseLong(pointExistValueObject.get("endTime").toString());

                countR += count;
                if (beginTimeR == 0 || beginTimeR > beginTime) {
                    beginTimeR = beginTime;
                }
                if (endTimeR == 0 || endTimeR < endTime) {
                    endTimeR = endTime;
                }
            }
            // value对象
            DBObject valueObject = new BasicDBObject();
            valueObject.put("count", countR);
            valueObject.put("time", HotSpotPointJob.create);//job创建时间
            valueObject.put("beginTime", beginTimeR);
            valueObject.put("endTime", endTimeR);

            pointObject.put("value", valueObject);
            hotspotResConnection.save(pointObject);
        }

        private void writePage(K key, V value) {

            long timeR = HotSpotPointJob.create;//job的创建时间
            //get the time and sum
            JSONObject vJSON = JSONObject.fromObject(value.toString());
            long beginTimeR = Long.parseLong(vJSON.get("beginTimeR").toString());
            long endTimeR = Long.parseLong(vJSON.get("endTimeR").toString());

            // 获取ID
            String id = key.toString().split(JobConst.ID_CONN)[0];
            // 获取key
            String keyStr = key.toString().split(JobConst.ID_CONN)[1];
            // 获取hotspot_res_id数据苦中的pages集合
            DBCollection hotspotResConnection = DBUtil.getDBCollection(DBNames.COLLECTION_RES_HOTSPOT + "_" + id, COLLECTION_NAME_PAGE);
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

        public synchronized void close(TaskAttemptContext context) throws IOException {
        }
    }
}
