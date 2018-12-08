package com.qianfeng.etl.mr.toHdfs;

import com.qianfeng.common.EventConstant;
import com.qianfeng.util.LogUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * 将解析出的数据存储到hdfs中的mapper中
 *
 */
public class LogToHdfsMapper extends Mapper<Object,Text,NullWritable,LogDataWritable> {

    private static final Logger logger = Logger.getLogger(LogToHdfsMapper.class);
    //输入\输出\过滤行记录
    private int inputRecords,outputRecords,filterRecords = 0;


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        this.inputRecords++;
        logger.info("输入的logger日志为"+value.toString());

        Map<String,String> logs = LogUtil.praserLog(value.toString());

        if (logs.isEmpty()){
            this.filterRecords++;
            return;
        }
        //获取事件

        String enventName = logs.get(EventConstant.EVENT_COLUMN_NAME_EVENT_NAME);
        EventConstant.EventEnum eventEnums = EventConstant.EventEnum.valuesOfValue(enventName);
        switch (eventEnums){
            case EVENT:
            case LAUNCH:
            case PAGEVIEW:
            case CHARGEREFUND:
            case CHARGEREQUEST:
            case CHARGESUCCESS:
//                将logs存储

                handleLogs(logs,enventName,context);

                break;
                default:
                    filterRecords++;
                    logger.warn("该envent比支持该类型"+enventName);
                    break;

        }

        super.map(key, value, context);
    }

    private void handleLogs(Map<String,String> logs, String enventName, Context context) {

        if (!logs.isEmpty()){

            //自定义的输入类型的数据和事件类型的数据的对接
            LogDataWritable ld = new LogDataWritable();
            ld.s_time  =logs.get(EventConstant.EVENT_COLUMN_NAME_SERVER_TIME);
            ld.en  =logs.get(EventConstant.EVENT_COLUMN_NAME_EVENT_NAME);
            ld.ver  =logs.get(EventConstant.EVENT_COLUMN_NAME_VERSION);
            ld.u_ud  =logs.get(EventConstant.EVENT_COLUMN_NAME_UUID);
            ld.u_mid  =logs.get(EventConstant.EVENT_COLUMN_NAME_MEMBER_ID);
            ld.u_sid  =logs.get(EventConstant.EVENT_COLUMN_NAME_SESSION_ID);
            ld.c_time  =logs.get(EventConstant.EVENT_COLUMN_NAME_CLIENT_TIME);
            ld.language  =logs.get(EventConstant.EVENT_COLUMN_NAME_LANGUAGE);
            ld.b_iev  =logs.get(EventConstant.EVENT_COLUMN_NAME_USERAGENT);
            ld.b_rst  =logs.get(EventConstant.EVENT_COLUMN_NAME_RESOLUTION);
            ld.p_url  =logs.get(EventConstant.EVENT_COLUMN_NAME_CURRENT_URL);
            ld.p_ref  =logs.get(EventConstant.EVENT_COLUMN_NAME_PREFFER_URL);
            ld.tt  =logs.get(EventConstant.EVENT_COLUMN_NAME_TITLE);
            ld.pl  =logs.get(EventConstant.EVENT_COLUMN_NAME_PLATFORM);
            ld.oid  =logs.get(EventConstant.EVENT_COLUMN_NAME_ORDER_ID);
            ld.on  =logs.get(EventConstant.EVENT_COLUMN_NAME_ORDER_NAME);
            ld.cut  =logs.get(EventConstant.EVENT_COLUMN_NAME_CURRENCY_TYPE);
            ld.cua  =logs.get(EventConstant.EVENT_COLUMN_NAME_CURRENCY_AMOUTN);
            ld.pt  =logs.get(EventConstant.EVENT_COLUMN_NAME_PAYMENT_TYPE);
            ld.ca  =logs.get(EventConstant.EVENT_COLUMN_NAME_EVENT_CATEGORY);
            ld.ac  =logs.get(EventConstant.EVENT_COLUMN_NAME_EVENT_ACTION);
            ld.kv_  =logs.get(EventConstant.EVENT_COLUMN_NAME_EVENT_KV);
            ld.du  =logs.get(EventConstant.EVENT_COLUMN_NAME_EVENT_DURATION);
            ld.os  =logs.get(EventConstant.EVENT_COLUMN_NAME_OS_NAME);
            ld.os_v  =logs.get(EventConstant.EVENT_COLUMN_NAME_OS_VERSION);
            ld.browser  =logs.get(EventConstant.EVENT_COLUMN_NAME_BROWSER_NAME);
            ld.browser_v  =logs.get(EventConstant.EVENT_COLUMN_NAME_BROWSER_VERSION);
            ld.country  =logs.get(EventConstant.EVENT_COLUMN_NAME_COUNTRY);
            ld.province  =logs.get(EventConstant.EVENT_COLUMN_NAME_PROVINCE);
            ld.city  =logs.get(EventConstant.EVENT_COLUMN_NAME_CITY);

            try {
            //数据的输出
            context.write(NullWritable.get(),ld);
            this.outputRecords++;

            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                this.filterRecords ++;
                logger.warn("写出到hdfs异常.",e);
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("输入、输出和过滤的记录数.inputRecords"+this.inputRecords
                +"  outputRecords:"+this.outputRecords+"  filterRecords:"+this.filterRecords);
    }
}
