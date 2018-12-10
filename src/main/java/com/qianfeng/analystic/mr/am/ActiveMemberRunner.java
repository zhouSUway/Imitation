package com.qianfeng.analystic.mr.am;

import com.google.common.collect.Lists;
import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.analystic.model.dim.value.reduce.MapperOutputValue;
import com.qianfeng.common.EventConstant;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.List;

public class ActiveMemberRunner implements Tool {


    private static final Logger logger = Logger.getLogger(ActiveMemberMapper.class);
    private Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new ActiveMemberRunner(),args);
        }catch (Exception e){
            logger.error("运行活跃会员指标失败",e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        //设置参数conf中
        this.setArgs(args,conf);

        //获取作业

        Job job = Job.getInstance(conf,"active_user");
        job.setJarByClass(ActiveMemberRunner.class);

        //初始化mapper 类
        //adddependency Jar ：true则是本地提交集群运行
        TableMapReduceUtil.initTableMapperJob(this.getScans(job),ActiveMemberMapper.class,
                StatsUserDimension.class,TimeOutputValue.class,job,false);


        //reducer的设置
        job.setReducerClass(ActiveMemberReuder.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(MapperOutputValue.class);


        //设置输出的格式类
        job.setOutputKeyClass(MapperOutputValue.class);
        return job.waitForCompletion(true)?0:1;

    }

    @Override
    public void setConf(Configuration configuration) {

        this.conf.addResource("query-mapping.xml");
        this.conf.addResource("output-writter.xml");
        this.conf = HBaseConfiguration.create(this.conf);

    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }


    //
    private void setArgs(String[] args,Configuration conf){

        String date = null;
        for (int i=0;i<args.length;i++){
            if (args[i].equals("-d")){
                if (i+1<args.length){
                    date = args[i+1];
                    break;
                }
            }
        }
        //代码到这儿 date 还是null 默认用昨天的时间
        if (date ==null){
            date=TimeUtil.getYesterdayDate();
        }

        //然后将date设置到时间conf中

        conf.set(GlobalConstants.RUNNING_DATE,date);


    }

    /**
     * 获取扫描的集合对象
     *
     */

    private List<Scan> getScans(Job job){
        Configuration conf = job.getConfiguration();
        //获取运行日期
        long start = Long.valueOf(TimeUtil.parserString2Long(conf.get(GlobalConstants.RUNNING_DATE)));
        long end = start+GlobalConstants.DAY_OF_MILISECONDS;
        //获取scand对象
        Scan scan = new Scan();

        scan.setStartRow(Bytes.toBytes(start+""));
        scan.setStopRow(Bytes.toBytes(end+""));


        //定义过滤器

        FilterList f1 = new FilterList();
        f1.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventConstant.HBASE_COLUMN_FAMILY),
                Bytes.toBytes(EventConstant.EVENT_COLUMN_NAME_EVENT_NAME),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(EventConstant.EventEnum.PAGEVIEW.alias)));

        //设置扫描的字段


        //定义过滤器链

        String [] fields ={
                EventConstant.EVENT_COLUMN_NAME_SERVER_TIME,
                EventConstant.EVENT_COLUMN_NAME_MEMBER_ID,
                EventConstant.EVENT_COLUMN_NAME_PLATFORM,
                EventConstant.EVENT_COLUMN_NAME_BROWSER_NAME,
                EventConstant.EVENT_COLUMN_NAME_BROWSER_VERSION,
                EventConstant.EVENT_COLUMN_NAME_EVENT_NAME
        };

        //将扫描的字段添加到filter中;

        f1.addFilter(this.getFilter(fields));
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,Bytes.toBytes(EventConstant.HBASE_TABLE_NAME));
        //将filter设置scan中
        scan.setFilter(f1);
        return Lists.newArrayList(scan);//谷歌的api

    }


    /**
     * 设置扫描的列
     */

    private MultipleColumnPrefixFilter getFilter(String[] fields){
        int length = fields.length;
        byte[][] filters = new byte[length][];
        for (int i=0;i<length;i++){
            fields[i]= String.valueOf(Bytes.toBytes(fields[i]));
        }
        return new MultipleColumnPrefixFilter(filters);
    }
}
