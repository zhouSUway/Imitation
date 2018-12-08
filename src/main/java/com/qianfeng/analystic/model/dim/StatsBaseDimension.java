package com.qianfeng.analystic.model.dim;

import com.qianfeng.analystic.model.dim.base.BaseDimension;

/**
 * 封装基础维度的顶端父类
 */
public abstract class StatsBaseDimension extends BaseDimension {


    public abstract BaseDimension getDateDimension();
}
