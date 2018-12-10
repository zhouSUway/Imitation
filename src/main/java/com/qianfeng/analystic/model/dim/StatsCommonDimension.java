package com.qianfeng.analystic.model.dim;

import com.qianfeng.analystic.model.dim.base.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 公共维度封装类，浏览器、 时间、平台、指标、维度
 */
public class StatsCommonDimension extends StatsBaseDimension {


    private PlatformDimension platformDimension = new PlatformDimension();
    private DateDimension dateDimension = new DateDimension();
    private KpiDimension kpiDimension = new KpiDimension();

    public StatsCommonDimension() {
    }


    public StatsCommonDimension(PlatformDimension platformDimension, DateDimension dateDimension, KpiDimension kpiDimension) {
        this.platformDimension = platformDimension;
        this.dateDimension = dateDimension;
        this.kpiDimension = kpiDimension;
    }

    protected static StatsCommonDimension clone(StatsCommonDimension statsCommonDimension) {

            return null;
    }


    @Override
    public int compareTo(BaseDimension o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        this.dateDimension.write(dataOutput);
        this.kpiDimension.write(dataOutput);
        this.platformDimension.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.dateDimension.readFields(dataInput);
        this.platformDimension.readFields(dataInput);
        this.kpiDimension.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsCommonDimension that = (StatsCommonDimension) o;
        return Objects.equals(platformDimension, that.platformDimension) &&
                Objects.equals(dateDimension, that.dateDimension) &&
                Objects.equals(kpiDimension, that.kpiDimension);
    }

    @Override
    public int hashCode() {

        return Objects.hash(platformDimension, dateDimension, kpiDimension);
    }

    public PlatformDimension getPlatformDimension() {
        return platformDimension;
    }

    public void setPlatformDimension(PlatformDimension platformDimension) {
        this.platformDimension = platformDimension;
    }

    public DateDimension getDateDimension(DateDimension dateDimensions) {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public KpiDimension getKpiDimension() {
        return kpiDimension;
    }

    public void setKpiDimension(KpiDimension kpiDimension) {
        this.kpiDimension = kpiDimension;
    }


    @Override
    public BaseDimension getDateDimension() {
        return null;
    }
}
