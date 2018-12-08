package com.qianfeng.analystic.mr.pv;

import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.value.BaseOutputValueWritable;
import com.qianfeng.analystic.mr.IOutputWriter;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PageViewReducer implements IOutputWriter {
    @Override
    public void outputWrite(Configuration conf, BaseDimension key, BaseOutputValueWritable value, PreparedStatement ps, IDimensionConvert convert) throws IOException, SQLException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;

    }
}
