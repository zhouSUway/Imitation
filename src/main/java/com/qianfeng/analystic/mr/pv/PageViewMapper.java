package com.qianfeng.analystic.mr.pv;

import com.qianfeng.analystic.model.dim.StatsCommonDimension;
import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.base.BrowserDimension;
import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.model.dim.base.KpiDimension;
import com.qianfeng.analystic.model.dim.base.PlatformDimension;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.common.DateEnum;
import com.qianfeng.common.EventConstant;
import com.qianfeng.common.KpiTypeEnum;
import com.qianfeng.util.LogUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.telnet.EchoOptionHandler;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public class PageViewMapper extends TableMapper<StatsUserDimension,TimeOutputValue> {


    private static final Logger logger = Logger.getLogger(PageViewMapper.class);
    StatsUserDimension ks = new StatsUserDimension();
    TimeOutputValue vs = new TimeOutputValue();
    private byte[] family = Bytes.toBytes(EventConstant.EVENT_COLUMN_NAME_EVENT_NAME);
    private KpiDimension PageViewKpi = new KpiDimension(KpiTypeEnum.PAGEVIEW.kpiName);


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        logger.info("打印日志:"+value.toString());
        String url = Bytes.toString(value.getValue(family,Bytes.toBytes(EventConstant.EVENT_COLUMN_NAME_CURRENT_URL)));
        String serviceTime = Bytes.toString(value.getValue(family,Bytes.toBytes(EventConstant.EVENT_COLUMN_NAME_SERVER_TIME)));
        String platition = Bytes.toString(value.getValue(family,Bytes.toBytes(EventConstant.EVENT_COLUMN_NAME_PLATFORM)));
        String browerName = Bytes.toString(value.getValue(family,Bytes.toBytes(EventConstant.EVENT_COLUMN_NAME_BROWSER_NAME)));
        String browerVerion = Bytes.toString(value.getValue(family,Bytes.toBytes(EventConstant.EVENT_COLUMN_NAME_BROWSER_VERSION)));

        //对三个字段进行判空
        if (StringUtils.isEmpty(url)||StringUtils.isEmpty(serviceTime)||StringUtils.isEmpty(platition)||StringUtils.isEmpty(browerName)||StringUtils.isEmpty(browerVerion)){
            logger.info("url:"+url+"serviceTime"+serviceTime+"platition"+platition+"browerName"+browerName+"browerVersion"+browerVerion);
            return;
        }

        //构建输出value
        Long serviceTimes = Long.valueOf(serviceTime);

        this.vs.setId(url);
        this.vs.setTime(serviceTimes);

//        构建输出的类

        List<PlatformDimension> platformDimensions = PlatformDimension.buildPlatform(platition);
        DateDimension dateDimension = DateDimension.buildDate(serviceTimes,DateEnum.DAY);
        List<BrowserDimension> browserDimensions = BrowserDimension.buildBrower(browerName, browerVerion);

        StatsCommonDimension statsCommonDimension = this.ks.getStatsCommonDimension();
//        为StatsCommonDimension赋值
        statsCommonDimension.getDateDimension(dateDimension);

        BrowserDimension browserDimension = new BrowserDimension("", "");

        //循环平台维度集合对象

        for (PlatformDimension pl :platformDimensions){
            statsCommonDimension.setPlatformDimension(pl);
            //循环输出的用于浏览器模块新增用户的指标
            for (BrowserDimension br:browserDimensions){
                statsCommonDimension.setKpiDimension(PageViewKpi);
                this.ks.setStatsCommonDimension(statsCommonDimension);
                this.ks.setBrowserDimension(br);
                //进行输出
                context.write(this.ks,this.vs);
            }
        }


    }
}

