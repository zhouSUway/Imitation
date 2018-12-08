package com.qianfeng.analystic.model.dim.value.map;

import com.qianfeng.analystic.model.dim.value.BaseOutputValueWritable;
import com.qianfeng.common.KpiTypeEnum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 用户模块和浏览器模块的map阶段的value的输出阶段
 * @return
 */

public class TimeOutputValue extends BaseOutputValueWritable {

    private  String id;///泛指 uuid sesionid  memberid
    private long time;//时间

    @Override
    public int compareTo(Object o) {
        if (this==o){
            return 0;
        }
        TimeOutputValue other = (TimeOutputValue) o;
        int tmp = this.id.compareTo(other.id);
        if (tmp!=0){
            return tmp;
        }

        long tmp1 = this.time - other.time;
        if (tmp1!=0){
            return (int) tmp1;
        }
        return tmp;
    }

    @Override
    public KpiTypeEnum getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(this.id);
        dataOutput.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.id = dataInput.readUTF();
        this.time = dataInput.readLong();

    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
