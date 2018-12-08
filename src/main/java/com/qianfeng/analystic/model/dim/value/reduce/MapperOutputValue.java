package com.qianfeng.analystic.model.dim.value.reduce;

import com.qianfeng.analystic.model.dim.value.BaseOutputValueWritable;
import com.qianfeng.common.KpiTypeEnum;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用户模块和浏览器模块下的reduce阶段下的value的输出类型
 */
public class MapperOutputValue extends BaseOutputValueWritable {

    private MapWritable value = new MapWritable();
    private KpiTypeEnum kpiTypeEnum;


    @Override
    public KpiTypeEnum getKpi() {
        return null;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.value.write(dataOutput);//map的写出
        WritableUtils.writeEnum(dataOutput,kpiTypeEnum);//枚举的写出
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.value.readFields(dataInput);//map的输出
        WritableUtils.readEnum(dataInput,KpiTypeEnum.class);//枚举的输出
    }

    public MapWritable getValue(){
        return value;
    }
    public MapWritable getMapWritable() {
        return value;
    }

    public void setMapWritable(MapWritable mapWritable) {
        this.value = mapWritable;
    }

    public KpiTypeEnum getKpiTypeEnum() {
        return kpiTypeEnum;
    }

    public void setKpiTypeEnum(KpiTypeEnum kpiTypeEnum) {
        this.kpiTypeEnum = kpiTypeEnum;
    }
}
