package com.qianfeng.analystic.mr.am;

import com.qianfeng.analystic.model.dim.StatsCommonDimension;
import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.base.BrowserDimension;
import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.model.dim.base.KpiDimension;
import com.qianfeng.analystic.model.dim.base.PlatformDimension;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.common.DateEnum;
import com.qianfeng.common.KpiTypeEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class ActiveMemberMapper extends Mapper<LongWritable,Text, StatsUserDimension, TimeOutputValue> {

    private static final Logger logger = Logger.getLogger(ActiveMemberMapper.class);

    private StatsUserDimension key = new StatsUserDimension();
    private TimeOutputValue value = new TimeOutputValue();
    private KpiDimension activeMemberKpi = new KpiDimension(KpiTypeEnum.ACTIVE_MEMBER.kpiName);
    private KpiDimension browerActeMemberKpi = new KpiDimension(KpiTypeEnum.BROWSER_ACTIVE_MEMBER.kpiName);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取需要的字段

        String line = value.toString();
        String splited[] = line.split("\001");
        String memberId = splited[12];
        String serviceTime = splited[15];
        String platform = splited[2];
        String browerName = splited[1];
        String browerVersion = splited[0];


        //对三个字段进行判空
        if (StringUtils.isEmpty(memberId) || StringUtils.isEmpty(serviceTime) || StringUtils.isEmpty(platform)) {
            logger.info("memebrid" + memberId + "service time" + serviceTime + "platform" + platform);
        }

        //构建输出的value
        Long serverTimeOfLong = Long.valueOf(serviceTime);
        this.value.setTime(serverTimeOfLong);
        this.value.setId(memberId);

        //构建输出的key
        List<PlatformDimension> platformDimensions = PlatformDimension.buildPlatform(platform);
        DateDimension dateDimension = DateDimension.buildDate(serverTimeOfLong, DateEnum.DAY);
        List<BrowserDimension> browserDimensions = BrowserDimension.buildBrower(browerName, browerVersion);

        //为statsCommonDimension赋值
        StatsCommonDimension statsCommonDimension = this.key.getStatsCommonDimension();

        BrowserDimension defaultBrower = new BrowserDimension("", "");
        //循环平台维度集合对象
        for (PlatformDimension pl : platformDimensions) {
            statsCommonDimension.setKpiDimension(activeMemberKpi);
            ;
            statsCommonDimension.setPlatformDimension(pl);
            this.key.setStatsCommonDimension(statsCommonDimension);
            this.key.setBrowserDimension(defaultBrower);

            //输出
            context.write(this.key, this.value);

            //该循环的输出用于浏览器模块的新增用户指标
            for (BrowserDimension br : browserDimensions) {
                statsCommonDimension.setKpiDimension(browerActeMemberKpi);
                this.key.setStatsCommonDimension(statsCommonDimension);
                this.key.setBrowserDimension(br);

                //写出
                context.write(this.key, this.value);
            }
        }
    }
}
