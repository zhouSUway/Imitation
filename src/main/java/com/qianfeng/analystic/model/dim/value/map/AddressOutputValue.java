package com.qianfeng.analystic.model.dim.value.map;

import com.qianfeng.analystic.model.dim.value.BaseOutputValueWritable;
import com.qianfeng.common.KpiTypeEnum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 地域模块的map输出阶段的valu的输出类型
 */
public class AddressOutputValue extends BaseOutputValueWritable {

    private String uuid = "";
    private String sessionid ="";

    public AddressOutputValue(String uuid, String sessionid) {
        this.uuid = uuid;
        this.sessionid = sessionid;
    }

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

        dataOutput.writeUTF(this.uuid);
        dataOutput.writeUTF(this.sessionid);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.uuid = dataInput.readUTF();
        this.sessionid = dataInput.readUTF();
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSessionid() {
        return sessionid;
    }

    public void setSessionid(String sessionid) {
        this.sessionid = sessionid;
    }


}
