package com.qianfeng.analystic.mr.pv;

import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.value.BaseOutputValueWritable;
import com.qianfeng.analystic.model.dim.value.reduce.MapperOutputValue;
import com.qianfeng.analystic.mr.IOutputWriter;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import com.qianfeng.common.EventConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PageViewOutputWriter implements IOutputWriter {

    private static final Logger logger = Logger.getLogger(PageViewOutputWriter.class);

    @Override
    public void outputWrite(Configuration conf, BaseDimension key, BaseOutputValueWritable value, PreparedStatement ps, IDimensionConvert convert) throws IOException, SQLException {

        StatsUserDimension statsUserDimension = (StatsUserDimension)key;
        MapperOutputValue v = (MapperOutputValue) value;
        int borwer_User = ((IntWritable)((MapperOutputValue)v).getValue().get(new IntWritable(-1))).get();

        //为pst赋值
        PreparedStatement pst =ps;
        IDimensionConvert ct = convert;
        Configuration cf = conf;

        int i = 0;
        pst.setInt(i++,ct.getDimensionInterfaceById(statsUserDimension.getBrowserDimension()));
        pst.setInt(i++,ct.getDimensionInterfaceById(statsUserDimension.getStatsCommonDimension().getDateDimension()));
        ps.setInt(i++,ct.getDimensionInterfaceById(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
        pst.setInt(i++,borwer_User);

        //添加到bacth
        pst.addBatch();
    }
}
