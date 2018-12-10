package com.qianfeng.analystic.mr.session;

import com.qianfeng.analystic.model.dim.StatsCommonDimension;
import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.base.BrowserDimension;
import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.model.dim.base.KpiDimension;
import com.qianfeng.analystic.model.dim.base.PlatformDimension;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.common.KpiTypeEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class SessionMapper extends TableMapper<StatsUserDimension,TimeOutputValue> {


    private static final Logger logger = Logger.getLogger(SessionMapper.class);
    StatsUserDimension key = new StatsUserDimension();
    TimeOutputValue value = new TimeOutputValue();

    KpiDimension newSession = new KpiDimension(KpiTypeEnum.SESSION.kpiName);
    KpiDimension kpiDimension = new KpiDimension();
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {


        //获取需要的字段

        String line = value.toString();
        String [] splied =line.split("\001");
        String sessionId = splied[1];
        String serverTime = splied[2];
        String platform = splied[3];
        String browerName = splied[4];
        String browerVersion =splied[5];

        //对三个地段进行判空

        if (StringUtils.isEmpty(sessionId)||StringUtils.isEmpty(serverTime)||StringUtils.isEmpty(platform)){

            logger.info("sessionId"+sessionId+"serverTime"+serverTime+"platform"+platform);

            return;
        }

        //构建输出的value
        long serverTimeOfLong = Long.valueOf(serverTime);
        this.value.setId(sessionId);
        this.value.setTime(serverTimeOfLong);

        //构建输出的key

        StatsCommonDimension statsCommonDimension = this.key.getStatsCommonDimension();
        //为statsCommonDimension赋值
        DateDimension dateDimension = new DateDimension();
        PlatformDimension platformDimension = new PlatformDimension();
        BrowserDimension browserDimension = new BrowserDimension("","");
        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setPlatformDimension(platformDimension);
        statsCommonDimension.setKpiDimension(newSession);

        this.key.setStatsCommonDimension(statsCommonDimension);
        this.key.setBrowserDimension(browserDimension);

        context.write(this.key,this.value);







    }
}
