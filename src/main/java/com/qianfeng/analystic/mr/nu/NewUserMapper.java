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
import org.apache.thrift.protocol.TJSONProtocol;

import java.io.IOException;
import java.util.List;

/**
 * 新增的用户
 * 总用户
 * 需要使用mapper类中的launch统计去重的uuid的个数
 *
 */
public class NewUserMapper extends TableMapper<StatsUserDimension,TimeOutputValue> {

    private static final Logger logger = Logger.getLogger(NewUserMapper.class);
    private StatsUserDimension key = new StatsUserDimension();
    private TimeOutputValue value = new TimeOutputValue();
    private KpiDimension newUserKpi = new KpiDimension(KpiTypeEnum.NEW_USER.kpiName);


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        //需要取得的字段

    String line = value.toString();
    String [] fields = line.split("\001");
    String uuid= fields[6];
    String sTime = fields[15];
    String platform = fields[2];
    String envent = fields[0];

        //对三个字段进行判空

   if (envent.equals(EventConstant.EventEnum.LAUNCH.alias));
        {
            logger.info("事件不是lanuch事件："+line);
        }
        //uuid和时间戳

        if (StringUtils.isEmpty(sTime)||StringUtils.isEmpty(uuid)){
            logger.info("stime is null and uid is null.sTime"+sTime+"uuid"+uuid);
        }

        //构建value
        Long serverTimeOfLong = Long.valueOf(sTime);
        this.value.setId(uuid);
        this.value.setTime(serverTimeOfLong);

        //1532593870123 2018-07-26 website 27F69684-BBE3-42FA-AA62-71F98E208


        //构建key
      DateDimension dateDimension = DateDimension.buildDate(serverTimeOfLong,DateEnum.DAY);
      PlatformDimension platformDimension = new PlatformDimension(platform);

      StatsCommonDimension statsCommonDimension = this.key.getStatsCommonDimension();

       //默认浏览器维度

        BrowserDimension defaultBrower = new BrowserDimension("","");
        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setKpiDimension(newUserKpi);
        statsCommonDimension.setPlatformDimension(platformDimension);

        this.key.setStatsCommonDimension(statsCommonDimension);
        this.key.setBrowserDimension(defaultBrower);

        //循环平台维度集合对象
        //该循环的输出用于浏览器模块的新增用户指标统计
        context.write(this.key,this.value);

    }
}
