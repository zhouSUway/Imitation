package com.qianfeng.etl.mr.toHbase;

import com.qianfeng.common.EventConstant;
import com.qianfeng.util.LogUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * 接解析后的数据存储到hbase的napper类
 */
public class LogToHbaseMapper extends Mapper<Object,Text,NullWritable,Put> {

    private static final Logger logger = Logger.getLogger(LogToHbaseMapper.class);
    private byte[] family = Bytes.toBytes(EventConstant.HBASE_COLUMN_FAMILY);

    //输入\输出\过滤行记录
    private int inputRecords,outputRecords,filterRecords = 0;
    private CRC32 crc32 = new CRC32();


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        logger.info("输出的日志为:"+value.toString());
        Map<String,String> info = new LogUtil().praserLog(value.toString());
        if (info.isEmpty()){
            this.inputRecords++;
            return;
        }

        //获取事件

        String enventName = info.get(EventConstant.EVENT_COLUMN_NAME_EVENT_NAME);
        EventConstant.EventEnum event = EventConstant.EventEnum.valuesOfValue(enventName);


        switch (event){
            case CHARGESUCCESS:
            case CHARGEREQUEST:
            case CHARGEREFUND:
            case PAGEVIEW:
            case LAUNCH:
            case EVENT:
                //将info储存
                handleInfo(info,enventName,context);
                break;
                default:
                    filterRecords++;
                    logger.warn("该数据不支持数据的清洗"+enventName);
                    break;

        }


    }

    private void handleInfo(Map<String, String> info, String enventName, Context context) {


    }
}
