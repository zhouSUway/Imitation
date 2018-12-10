package com.qianfeng.analystic.mr.session;

import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.base.PlatformDimension;
import com.qianfeng.analystic.model.dim.value.BaseOutputValueWritable;
import com.qianfeng.analystic.model.dim.value.reduce.MapperOutputValue;
import com.qianfeng.analystic.mr.IOutputWriter;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import com.qianfeng.common.EventConstant;
import com.qianfeng.common.KpiTypeEnum;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.awt.image.Kernel;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SessionOutputWritter implements IOutputWriter {
    @Override
    public void outputWrite(Configuration conf, BaseDimension key, BaseOutputValueWritable value, PreparedStatement ps, IDimensionConvert convert) throws IOException, SQLException {
        MapperOutputValue v = (MapperOutputValue)value;
        StatsUserDimension statsUserDimension =(StatsUserDimension) key;
        int sessions = ((IntWritable)((MapperOutputValue) value).getValue().get(new IntWritable(-1))).get();
        int sessionsLength = ((IntWritable) ((MapperOutputValue) value).getValue().get(new IntWritable(-2))).get();

        //为ps赋值
        int i=0;
        ps.setInt(i++,convert.getDimensionInterfaceById(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
        ps.setInt(i++,convert.getDimensionInterfaceById(statsUserDimension.getStatsCommonDimension().getDateDimension()));
        if (v.getKpi().equals(KpiTypeEnum.BROWSER_SESSION))
        {
            ps.setInt(i++,convert.getDimensionInterfaceById(statsUserDimension.getStatsCommonDimension().getKpiDimension()));

        }

        ps.setInt(++i,sessions);
        ps.setInt(++i,sessionsLength);
        ps.setInt(++i,sessionsLength);

        //添加到批次处理中
        ps.addBatch();


    }
}
