package com.landray.behavior.job.request;

import com.landray.behavior.job.base.BehaviorJob;
import com.landray.behavior.job.hotspot.HotSpotWidgetJob;
import com.landray.behavior.request.input.RequestLogUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * 数据转换job
 * Created by nemo on 15-9-7.
 */
public class RequestTransformJob extends BehaviorJob{
    public String getJobCate() {
        return "request";
    }
    public static final String JOB_NAME = "transform";

    public String getJobName() {
        return JOB_NAME;
    }

    public Class getMapperClass() {
        return TransformMapper.class;
    }

    public Class getReducerClass() {
        return TransformReducer.class;
    }

    public Class<? extends OutputFormat> getOutputClass() {
        return TextOutputFormat.class;
    }

    public Class<?> getJarClass() {
        return HotSpotWidgetJob.class;
    }

    public static class TransformMapper extends  BehaviorMapper{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String logStrHIVE = RequestLogUtil.getLogStrHIVE(value.toString());
            if(logStrHIVE == null){
                return;
            }
            context.write(new Text(key.toString()),new Text(logStrHIVE));
        }
    }

    public static class TransformReducer extends BehaviorReducer{
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator i$ = values.iterator();
            while(i$.hasNext()) {
                Object value = i$.next();
                context.write(new Text(value.toString()),new Text());
            }
        }
    }
}
