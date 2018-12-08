package com.qianfeng.analystic.mr.nu;


import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.model.dim.value.reduce.MapperOutputValue;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import com.qianfeng.analystic.mr.service.impl.IDimensionConvertImpl;
import com.qianfeng.common.DateEnum;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.util.JdbcUtil;
import com.qianfeng.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * truncate dimension_browser;
 * truncate dimension_currency_type;
 * truncate dimension_date;
 * truncate dimension_event;
 * truncate dimension_inbound;
 * truncate dimension_kpi;
 * truncate dimension_location;
 * truncate dimension_os;
 * truncate dimension_payment_type;
 * truncate dimension_platform;
 * truncate event_info;
 * truncate order_info;
 * truncate stats_device_browser;
 * truncate stats_device_location;
 * truncate stats_event;
 * truncate stats_hourly;
 * truncate stats_inbound;
 * truncate stats_order;
 * truncate stats_user;
 * truncate stats_view_depth;
 * <p>
 * 新增用户的runnerlei
 */
public class NewUserRunner implements Tool {

    private static Logger logger = Logger.getLogger(NewUserRunner.class);
    private Configuration conf = new Configuration();


    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new NewUserRunner(), args);
        } catch (Exception e) {
            logger.warn("执行有误tools", e);
        }
    }


    @Override
    public void setConf(Configuration conf) {
        this.conf.addResource("query-mapping.xml");
        this.conf.addResource("output-writter.xml");
        this.conf.addResource("total-mapping.xml");
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {

        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();

        //处理的参数
        this.handleAgrs(args, conf);

        //并获取job的设置

        Job job = Job.getInstance();
        job.setJarByClass(NewUserRunner.class);

        //map端
        job.setMapperClass(NewUserMapper.class);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(MapperOutputValue.class);

//        设置输出路径

        job.setReducerClass(NewUserReducer.class);

        //        return job.waitForCompletion(true)?0:1;
        if (job.waitForCompletion(true)) {
            this.computeNewTotalUser(job);
            return 0;
        } else {
            return 1;
        }
    }


    /**
     * 计算新增的总用户
     * <p>
     * 1、获取运行当天的日期，然后再获取到运行当天前一天的日期，然后获取对应时间维度Id
     * 2、当对应时间维度Id都大于0，则正常计算：查询前一天的新增总用户，获取当天的新增用户
     *
     * @param job
     */
    private void computeNewTotalUser(Job job) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //获取运行的日期
            String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
            long nowDay = TimeUtil.parserString2Long(date);
            long yesterDay = nowDay - GlobalConstants.DAY_OF_MILISECONDS;

            //获取对应的时间维度
            DateDimension nowDateDimension = DateDimension.buildDate(nowDay, DateEnum.DAY);
            DateDimension yesterdayDateDimension = DateDimension.buildDate(yesterDay, DateEnum.DAY);

            int nowDimensionId = -1;
            int yesterDimensionId = -1;

            //获取维度的id
            IDimensionConvert convert = new IDimensionConvertImpl();
            nowDimensionId = convert.getDimensionInterfaceById(nowDateDimension);
            yesterDimensionId = convert.getDimensionInterfaceById(yesterdayDateDimension);

            //判断昨天和今天的维度Id是否大于0
            conn = JdbcUtil.getConn();
            Map<String, Integer> map = new HashMap<String, Integer>();
            if (yesterDimensionId > 0) {
                ps = conn.prepareStatement(conf.get(GlobalConstants.PREFIX_TOTAL + "new_total_user"));
                //赋值
                ps.setInt(1, yesterDimensionId);
                //执行
                rs = ps.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int totalNewUser = rs.getInt("total_install_users");
                    //存储
                    map.put(platformId + "", totalNewUser);
                }
            }

            if (nowDimensionId > 0) {
                ps = conn.prepareStatement(conf.get(GlobalConstants.PREFIX_TOTAL + "user_new_user"));
                //赋值
                ps.setInt(1, nowDimensionId);
                //执行
                rs = ps.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int newUser = rs.getInt("new_install_users");
                    //存储
                    if (map.containsKey(platformId + "")) {
                        newUser += map.get(platformId + "");
                    }
                    map.put(platformId + "", newUser);
                }
            }

            //更新新增的总用户
            ps = conn.prepareStatement(conf.get(GlobalConstants.PREFIX_TOTAL + "user_new_update_user"));
            //赋值
            for (Map.Entry<String, Integer> en : map.entrySet()) {
                ps.setInt(1, nowDimensionId);
                ps.setInt(2, Integer.parseInt(en.getKey()));
                ps.setInt(3, en.getValue());
                ps.setString(4, conf.get(GlobalConstants.RUNNING_DATE));
                ps.setInt(5, en.getValue());
                ps.execute();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                JdbcUtil.getClosed(conn, ps, rs);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //设置map阶段的输入路径
    private void setInputOutputPath(Job job) {

        String data = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        String field[] = data.split("-");
        Path outPath = new Path("/ods/month=" + field[1] + "/day=" + field[2]);

        try {
            FileSystem fs = FileSystem.get(conf);

            if (fs.exists(outPath)) {

                fs.delete(outPath, true);
            }
            FileInputFormat.setInputPaths(job, outPath);
        } catch (IOException e) {
            logger.warn("配置信息有误", e);
        }

    }


    /**
     * 将收到的日期存储在conf中，以供后续使用
     * 如果没有传递日期则默认是昨天的日志
     *
     * @param agrs
     * @param conf
     */
    private void handleAgrs(String[] agrs, Configuration conf) {

        String date = null;
        for (int i = 0; i < agrs.length; i++) {
            if (agrs[i].equals("-d")) {
                if (i + 1 < agrs.length) {
                    date = agrs[i + 1];
                    break;
                }
            }
        }
        //代码到这儿，date还是null，默认用昨天的时间
        if (date == null) {

            date = TimeUtil.getYesterdayDate(date);
        }
        conf.set(GlobalConstants.RUNNING_DATE, date);
    }
}
