package com.qianfeng.analystic.mr.service;

import com.qianfeng.analystic.model.dim.base.BaseDimension;

import java.io.IOException;
import java.sql.SQLException;

/**
 * 更具维度的对象获取维度的接口id
 */
public interface IDimensionConvert  {

    /**
     * 获取维度的接口以及id
     * @param baseDimension
     * @return
     * @throws IOException
     * @throws SQLException
     */
    int getDimensionInterfaceById(BaseDimension baseDimension)throws IOException,SQLException;


}
