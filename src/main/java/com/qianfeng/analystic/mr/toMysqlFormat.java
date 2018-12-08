package com.qianfeng.analystic.mr;

import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.value.BaseOutputValueWritable;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import com.qianfeng.analystic.mr.service.impl.IDimensionConvertImpl;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.common.KpiTypeEnum;
import com.qianfeng.util.JdbcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 自定义到MySQL的输出格式
 *
 */
public class toMysqlFormat extends OutputFormat<BaseDimension,BaseOutputValueWritable> {

    private static final Logger logger = Logger.getLogger(toMysqlFormat.class);

    @Override
    public RecordWriter<BaseDimension, BaseOutputValueWritable> getRecordWriter(TaskAttemptContext Context) throws IOException, InterruptedException {
        Connection conn = JdbcUtil.getConn();
        Configuration conf = Context.getConfiguration();
        IDimensionConvert convert = new IDimensionConvertImpl();

        return new IOutputReaderWriter(conn,conf,convert);
    }

    /**
     * 自定封装类，用于输出值
     * @param
     * @throws IOException
     * @throws InterruptedException
     */
    public class IOutputReaderWriter extends RecordWriter<BaseDimension,BaseOutputValueWritable>{

        private  Connection conn = null;
        private Configuration conf=null;
        private IDimensionConvert convert=null;
        //储存kpi kpi对应的kpi
        private Map<KpiTypeEnum,PreparedStatement> map = new HashMap<KpiTypeEnum,PreparedStatement>();
        //存储kpi :count对应的累加数
        private Map<KpiTypeEnum,Integer> batch = new HashMap<KpiTypeEnum,Integer>();

        public IOutputReaderWriter(Connection conn,Configuration conf,IDimensionConvert convert){
            this.conf=conf;
            this.conn=conn;
            this.convert=convert;
        }

        /**
         * 将结果输出的核心方法
         * @param
         * @param
         * @throws IOException
         * @throws InterruptedException
         */

        @Override
        public void write(BaseDimension key, BaseOutputValueWritable value) throws IOException, InterruptedException {
            if (key==null||value==null){
                return;
            }

            PreparedStatement pst = null;
            try {
                //获取kpi 根据kpi来获取对应的sql
                KpiTypeEnum kpi = value.getKpi();
                int count =1;
                //从map中获取是否有对应的pst
                if (map.get(kpi)==null){
                    //从conf获取对应的kpi应为conf中会有存储的sql
                    pst=this.conn.prepareStatement(conf.get(kpi.kpiName));
                    map.put(kpi,pst);
                }else {
                    pst = map.get(kpi);
                    count=batch.get(kpi);
                    count++;
                }

                //将count存储
                batch.put(kpi,count);

                //为pst赋值
                String writerName = conf.get(GlobalConstants.PREFIX_OUTPUT+kpi.kpiName);
                Class<?> classz = Class.forName(writerName);//转换成类
                IOutputWriter iOutputWriter = (IOutputWriter) classz.newInstance();

                iOutputWriter.outputWrite(conf,key,value,pst,convert);


                //当到达一定的batch就可以触发执行

                if (count%GlobalConstants.NUM_OF_BATCH==0){
                    pst.executeBatch();//批次zhix
                    conn.commit();
                    batch.remove(kpi);
                }
            }catch (Exception e){
                logger.warn("执行getRecordeWriter方法失败",e);
            }
        }

        /**
         * 关闭该关闭的对象
         * @param taskAttemptContext
         * @throws IOException
         * @throws InterruptedException
         */

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            try {
                //循环将剩下的map中的pst进行执行
                for (Map.Entry<KpiTypeEnum,PreparedStatement> en:map.entrySet()) {

                    en.getValue().executeBatch();
                }
            }catch (Exception e){
                logger.warn("批量执行剩余的pst异常",e);
            }finally {
                if (conn!=null){
                    try {
                        conn.close();
                    }catch (Exception e){
                        //关闭连接抛出异常
                    }finally {
                        //循环将剩余的map在pst进行关闭
                        for (Map.Entry<KpiTypeEnum,PreparedStatement>en : map.entrySet()){

                            try {
                                en.getValue().executeBatch();
                            } catch (SQLException e) {
                                logger.warn("循环关闭pst异常",e);
                            }

                        }
                    }
                }
            }
        }
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

        //TODO donothing
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext Context) throws IOException, InterruptedException {
        return new FileOutputCommitter(null,Context);
    }
}
