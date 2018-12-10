package com.qianfeng.analystic.mr.am;

import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.value.BaseOutputValueWritable;
import com.qianfeng.analystic.model.dim.value.reduce.MapperOutputValue;
import com.qianfeng.analystic.mr.IOutputWriter;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.common.KpiTypeEnum;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ActiveMemberOuputWritter implements IOutputWriter {
    @Override
    public void outputWrite(Configuration conf, BaseDimension key, BaseOutputValueWritable value, PreparedStatement ps, IDimensionConvert convert) throws IOException, SQLException {

        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapperOutputValue v= (MapperOutputValue) value;
        int newUser = ((IntWritable)((MapperOutputValue) value).getValue().get(new IntWritable(-1))).get();

        //给pst赋值
        int i=0;
        ps.setInt(++i,convert.getDimensionInterfaceById(statsUserDimension.getStatsCommonDimension().getDateDimension()));
        ps.setInt(++i,convert.getDimensionInterfaceById(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));

        if (v.getKpi().equals(KpiTypeEnum.BROWSER_ACTIVE_MEMBER)){
            ps.setInt(++i,convert.getDimensionInterfaceById(statsUserDimension.getBrowserDimension()));
        }

        ps.setInt(++i,newUser);
        ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
        ps.setInt(++i,newUser);

        //添加到批次处理中
        ps.addBatch();

    }
}
