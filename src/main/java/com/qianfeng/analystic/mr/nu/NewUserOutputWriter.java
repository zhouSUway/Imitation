package com.qianfeng.analystic.mr.nu;

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

/**
 * 新增用户
 *给pst赋值
 */
public class NewUserOutputWriter implements IOutputWriter {
    @Override
    public void outputWrite(Configuration conf, BaseDimension key, BaseOutputValueWritable value, PreparedStatement ps, IDimensionConvert convert) throws IOException, SQLException {

        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapperOutputValue v = (MapperOutputValue) value;
        int newUsers = ((IntWritable) ((MapperOutputValue)v).
                getValue().get(new IntWritable(-1))).get();

        //为ps赋值

        int i= 0;
        ps.setInt(++i,convert.getDimensionInterfaceById(statsUserDimension.getStatsCommonDimension().getDateDimension()));
        ps.setInt(++i,convert.getDimensionInterfaceById(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));

        if (v.getKpi().equals(KpiTypeEnum.BROWSER_NEW_USER)){
            ps.setInt(++i,convert.getDimensionInterfaceById(statsUserDimension.getBrowserDimension()));
        }
        ps.setInt(++i,newUsers);
        ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
        ps.setInt(++i,newUsers);

        //添加到批次处理中
        ps.addBatch();

    }
}
