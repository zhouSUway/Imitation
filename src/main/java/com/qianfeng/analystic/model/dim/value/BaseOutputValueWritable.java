package com.qianfeng.analystic.model.dim.value;

import com.qianfeng.common.KpiTypeEnum;
import org.apache.hadoop.io.Writable;

import javax.jnlp.PersistenceService;

/**
 * map与reduc阶段的输出value的类型的顶级父类
 *
 */
public abstract class BaseOutputValueWritable implements Writable {

    /**
     * 获取kpi
     */

    public abstract KpiTypeEnum getKpi();//获取一个kpi的抽象方法


    public abstract int compareTo(Object o);

}
