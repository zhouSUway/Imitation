package com.qianfeng.analystic.mr;

import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.value.BaseOutputValueWritable;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 *
 * 将reduce阶段的统计的结果直接输出到mysql
 */
public interface IOutputWriter {

    /**
     * 操作接口
     * @param conf
     * @param key
     * @param value
     * @param ps
     * @param convert
     * @throws IOException
     * @throws SQLException
     */
    void outputWrite(Configuration conf, BaseDimension key, BaseOutputValueWritable value,
                     PreparedStatement ps, IDimensionConvert convert) throws IOException,SQLException;


}
