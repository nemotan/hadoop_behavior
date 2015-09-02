package com.landray.behavior.job.hotspot;

import com.landray.behavior.util.DateUtil;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.io.IOException;

/**
 * @author nemo
 */
public class HotSpotPointJob extends AbstractHotSpotJob {
    //job创建时间，每次初始化的时候，该create重新赋值
    public static long create;

    public HotSpotPointJob() {
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

    public Class<? extends OutputFormat> getOutputClass() {
        return HotSpotPointOutputFormat.class;
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
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String keyStr = key.toString().split(ID_CONN)[1];
            JSONObject keyJSON = JSONObject.fromObject(keyStr);
            if (keyJSON.containsKey("user")) {
                //point
                reducePoint(key, values, context);
            } else {
                //page
                reducePage(key, values, context);
            }
        }

        protected void reducePoint(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long countR = 0l;
            long beginTimeR = 0l;
            long endTimeR = 0l;

            for (Text value : values) {
                // 累加
                JSONObject valueJson = JSONObject.fromObject(value.toString());
                long count = Long.parseLong(valueJson.get("count").toString());
                long beginTime = Long.parseLong(valueJson.get("beginTime").toString());
                long endTime = Long.parseLong(valueJson.get("endTime").toString());

                countR += count;
                if (beginTimeR == 0 || beginTimeR > beginTime) {
                    beginTimeR = beginTime;
                }
                if (endTimeR == 0 || endTimeR < endTime) {
                    endTimeR = endTime;
                }

            }
            JSONObject vJSON = new JSONObject();
            vJSON.put("countR", countR);
            vJSON.put("beginTimeR", beginTimeR);
            vJSON.put("endTimeR", endTimeR);
            context.write(key, new Text(vJSON.toString()));
        }


        protected void reducePage(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long beginTimeR = 0l;
            long endTimeR = 0l;

            for (Text value : values) {
                JSONObject valueJson = JSONObject.fromObject(value.toString());
                long beginTime = Long.parseLong(valueJson.get("beginTime").toString());
                long endTime = Long.parseLong(valueJson.get("endTime").toString());

                if (beginTimeR == 0 || beginTimeR > beginTime) {
                    beginTimeR = beginTime;
                }
                if (endTimeR == 0 || endTimeR < endTime) {
                    endTimeR = endTime;
                }

            }
            JSONObject vJSON = new JSONObject();
            vJSON.put("beginTimeR", beginTimeR);
            vJSON.put("endTimeR", endTimeR);
            context.write(key, new Text(vJSON.toString()));
        }
    }

}