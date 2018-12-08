package com.qianfeng.analystic.model.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 按照小时的活跃度用户，小时的session的个数
 */
public class KpiDimension extends BaseDimension {
    private int id;
    private String kpiName;

    public  KpiDimension(){}

    public KpiDimension(String kpiName) {
        this.kpiName = kpiName;
    }

    public KpiDimension(int id, String kpiName) {
        this.id = id;
        this.kpiName = kpiName;
    }

    /**
     * 重写比较器
     * @param o
     * @return
     */
    @Override
    public int compareTo(BaseDimension o) {
        if (this==o){
            return 0;
        }
        KpiDimension other = (KpiDimension) o;
        int tmp = this.id-other.id;
        if (tmp!=0){
            return tmp;
        }
        tmp = this.kpiName.compareTo(other.kpiName);
        return tmp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        /**
         * 序列化数据
         */
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.kpiName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        /**
         * 反序列化数据
         */
        this.id = dataInput.readInt();
        this.kpiName = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KpiDimension that = (KpiDimension) o;
        return id == that.id &&
                Objects.equals(kpiName, that.kpiName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, kpiName);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getKpiName() {
        return kpiName;
    }

    public void setKpiName(String kpiName) {
        this.kpiName = kpiName;
    }
}
