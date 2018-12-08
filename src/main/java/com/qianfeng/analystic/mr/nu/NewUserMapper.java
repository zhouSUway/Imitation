package com.qianfeng.analystic.mr.nu;

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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * 新增的用户
 * 总用户
 * 需要使用mapper类中的launch统计的uuid的个数
 *
 */
public class NewUserMapper extends TableMapper<StatsUserDimension,TimeOutputValue> {

    private static final Logger logger = Logger.getLogger(NewUserMapper.class);
    private byte[] family = Bytes.toBytes(EventConstant.HBASE_COLUMN_FAMILY);
    private StatsUserDimension sud = new StatsUserDimension();
    private TimeOutputValue tov = new TimeOutputValue();
    private KpiDimension newUserKpi = new KpiDimension(KpiTypeEnum.NEW_USER.kpiName);
    private KpiDimension browerNewUserKpi  = new KpiDimension(KpiTypeEnum.BROWSER_NEW_USER.kpiName);


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        //需要取得的字段

        String uuid = Bytes.toString(value.getValue(family,Bytes.toBytes(EventConstant.EVENT_COLUMN_NAME_UUID)));
        String serverTime = Bytes.toString(value.getValue(family,Bytes.toBytes(EventConstant.EVENT_COLUMN_NAME_SERVER_TIME)));
        String platform  = Bytes.toString(value.getValue(family,Bytes.toBytes(EventConstant.EVENT_COLUMN_NAME_PLATFORM)));
        String browserName = Bytes.toString(value.getValue(family,Bytes.toBytes(EventConstant.EVENT_COLUMN_NAME_BROWSER_NAME)));
        String browerVersion = Bytes.toString(value.getValue(family,Bytes.toBytes(EventConstant.EVENT_COLUMN_NAME_BROWSER_VERSION)));

        //对三个字段进行判空

        if (StringUtils.isEmpty(uuid)||StringUtils.isEmpty(serverTime)||StringUtils.isEmpty(platform)
                ||StringUtils.isEmpty(browserName)||StringUtils.isEmpty(browerVersion)){
            logger.warn("uuid"+uuid+"serverTime"+serverTime+"platform"+platform+"browerVersion"+
            browerVersion);
            return;
        }

        //构建value
        Long serverTimeOfLong = Long.valueOf(serverTime);
        this.tov.setId(uuid);
        this.tov.setTime(serverTimeOfLong);

        //1532593870123 2018-07-26 website 27F69684-BBE3-42FA-AA62-71F98E208


        //构建输出的key
        List<PlatformDimension> platformDimensions = PlatformDimension.buildPlatform(platform);
        DateDimension dateDimensions = DateDimension.buildDate(serverTimeOfLong, DateEnum.DAY);
        List<BrowserDimension> browserDimensions = BrowserDimension.buildBrower(browserName, browerVersion);

        StatsCommonDimension statsCommonDimension = this.sud.getStatsCommonDimension();

//        为statsCommonDimension赋值

        statsCommonDimension.getDateDimension(dateDimensions);
        BrowserDimension browserDimension = new BrowserDimension("", "");

        //循环平台维度集合对象

        for (PlatformDimension platformDimension:platformDimensions){
            statsCommonDimension.setKpiDimension(newUserKpi);
            statsCommonDimension.setPlatformDimension(platformDimension);
            this.sud.setStatsCommonDimension(statsCommonDimension);
            this.sud.setBrowserDimension(browserDimension);
        }



        //输出
        context.write(this.sud,this.tov);
        //该循环的输出用于浏览器模块的新增用户指标统计
        for (BrowserDimension br :browserDimensions){
            statsCommonDimension.setKpiDimension(browerNewUserKpi);
            this.sud.setStatsCommonDimension(statsCommonDimension);
            this.sud.setBrowserDimension(br);
        }
    }
}
