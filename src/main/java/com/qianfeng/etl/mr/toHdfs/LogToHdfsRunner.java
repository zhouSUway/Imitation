package com.qianfeng.etl.mr.toHdfs;

import com.qianfeng.common.GlobalConstants;
import com.qianfeng.util.TimeUtil;
import com.sun.istack.internal.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 驱动类
 */
public class LogToHdfsRunner implements Tool {

    private static  final Logger logger = Logger.getLogger(LogToHdfsRunner.class);

    Configuration conf = null;


    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new LogToHdfsRunner(),args);
        } catch (Exception e) {
            logger.warning("执行hdfs Runner异常");
        }
    }

    /**
     * yarn jar /home/gp1706.jar com.qianfeng.etl.mr.tohbase.ToLogRunner -d 2018-012-4
     * @param
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] agrs) throws Exception {

        Configuration conf = this.getConf();

        //设置处理的参数

        this.handleAgrs(agrs,conf);

        //获取并设置job
        Job  job = Job.getInstance(conf,"TODHFS");
        job.setJarByClass(LogToHdfsRunner.class);

        //设置map端
        job.setMapperClass(LogToHdfsMapper.class);
        job.setMapOutputKeyClass(LogDataWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置reduce Task的个数
        job.setNumReduceTasks(0);

        //设置map阶段的输入路径

        this.setInputOutputPath(job);

        return job.waitForCompletion(true)? 0:1;
    }

    //设置map阶段的输入路径
    private void setInputOutputPath(Job job) {

        String data = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        String field[] = data.split("-");
        Path intPath = new Path("/logs/" + field[1] + "/" + field[2]);
        Path outPath = new Path("/ods/month=" + field[1] + "/day=" + field[2]);


        try {
            FileSystem fs = FileSystem.get(conf);

            if (fs.exists(intPath)){
                FileInputFormat.addInputPath(job,intPath);
            }else {
                throw new RuntimeException("数据数据路径不存在");
            }
            if (fs.exists(outPath)){

                fs.delete(outPath,true);
            }
            FileOutputFormat.setOutputPath(job,outPath);
        } catch (IOException e) {
            logger.warning("配置信息有误");
        }

    }

    /**
     * 将收到的日期存储在conf中，以供后续使用
     * 如果没有传递日期则默认是昨天的日志
     * @param agrs
     * @param conf
     */
    private void handleAgrs(String[] agrs, Configuration conf) {

        String date = null;
        for (int i=0;i<agrs.length;i++){
            if (agrs[i].equals("-d")){
                if (i+1<agrs.length){
                   date=agrs[i+1];
                   break;
                }
            }
        }
        //代码到这儿，date还是null，默认用昨天的时间
        if (date==null){

            date = TimeUtil.getYesterdayDate(date);
        }
        conf.set(GlobalConstants.RUNNING_DATE,date);
    }

    @Override
    public void setConf(Configuration conf) {

        this.conf = HBaseConfiguration.create();
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
