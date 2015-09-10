package com.landray.behavior.job.base;

import com.landray.behavior.base.util.HDFSUtil;
import com.landray.behavior.job.hotspot.AbstractHotSpotJob;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by nemo on 15-9-2.
 */
public abstract class BehaviorJob {
    protected static final Log LOG = LogFactory.getLog(BehaviorJob.class);

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
        String inputPath = JobConst.ADDRESS+"/input/logs/" + month + "/" + getJobCate() + "." + date + "*";
        String outPath = JobConst.ADDRESS+"/output/logs/" + date + "/" + getJobCate() + "_" + getJobName();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, getJobCate() + "_" + getJobName() + "_" + date);
        job.setJarByClass(getJarClass());
        job.setMapperClass(getMapperClass());
        job.setReducerClass(getReducerClass());
        job.setOutputFormatClass(getOutputClass());
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

    public abstract static class BehaviorMapper extends Mapper<Object, Text, Text, Text> {}

    public abstract static class BehaviorReducer extends Reducer<Text, Text, Text, Text> {}
    /**
     * 获取job的种类 for： hotspot|request
     *
     * @return
     */
    public abstract  String getJobCate();
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
     * 获取output format
     *
     * @return
     */
    public abstract Class<? extends OutputFormat> getOutputClass();
    /**
     *
     * 获取jar class
     * @return
     */
    public abstract Class<?> getJarClass();
}
