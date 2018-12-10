package com.qianfeng.analystic.mr.pv;

import com.aliyun.odps.utils.StringUtils;
import com.qianfeng.analystic.model.dim.StatsCommonDimension;
import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.base.BrowserDimension;
import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.model.dim.base.KpiDimension;
import com.qianfeng.analystic.model.dim.base.PlatformDimension;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.common.KpiTypeEnum;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import org.apache.log4j.Logger;

import java.io.IOException;


public class PageViewMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {


    private static final Logger logger = Logger.getLogger(PageViewMapper.class);
    StatsUserDimension key = new StatsUserDimension();
    TimeOutputValue value = new TimeOutputValue();
    private KpiDimension PageViewKpi = new KpiDimension(KpiTypeEnum.PAGEVIEW.kpiName);


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        logger.info("打印日志:" + value.toString());
        String line = value.toString();
        String splied[] = line.split("\001");

        String url = splied[1];
        String serviceTime = splied[2];
        String platition = splied[3];
        String browerName = splied[4];
        String browerVerion = splied[5];

        //对三个字段进行判空
        if (StringUtils.isEmpty(url) || StringUtils.isEmpty(serviceTime) || StringUtils.isEmpty(platition) || StringUtils.isEmpty(browerName) || StringUtils.isEmpty(browerVerion)) {
            logger.info("url:" + url + "serviceTime" + serviceTime + "platition" + platition + "browerName" + browerName + "browerVersion" + browerVerion);
            return;
        }

        //构建输出value
        Long serviceTimes = Long.valueOf(serviceTime);
        this.value.setId(url);
        this.value.setTime(serviceTimes);

//        构建输出key
        PlatformDimension platformDimension = new PlatformDimension();
        DateDimension dateDimension = new DateDimension();
        StatsCommonDimension statsCommonDimension = this.key.getStatsCommonDimension();
//        为StatsCommonDimension赋值
        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setPlatformDimension(platformDimension);
        statsCommonDimension.setKpiDimension(PageViewKpi);
        BrowserDimension browserDimension = new BrowserDimension("", "");

        //循环平台维度集合对象

        this.key.setBrowserDimension(browserDimension);
        this.key.setStatsCommonDimension(statsCommonDimension);

        context.write(this.key, this.value);


    }
}

