package com.qianfeng.analystic.model.dim;

import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.base.BrowserDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 封装用户模块和浏览器模块中的map与reduce中的key的输出类型
 */
public class StatsUserDimension extends StatsBaseDimension {

    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private BrowserDimension browserDimension = new BrowserDimension();

    public StatsUserDimension(){}

    public StatsUserDimension(StatsCommonDimension statsCommonDimension, BrowserDimension browserDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.browserDimension = browserDimension;
    }

    /**
     * 克隆当前一个实例
     * @param dimension
     * @return
     */

    public static StatsUserDimension clone(StatsUserDimension dimension){

        return null;

    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this==o){
            return 0;
        }

        StatsUserDimension other = (StatsUserDimension) o;
        int tmp = this.browserDimension.compareTo(other.browserDimension);
        if (tmp!=0){
            return tmp;
        }
        tmp = this.statsCommonDimension.compareTo(other.browserDimension);
        return tmp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        this.statsCommonDimension.write(dataOutput);
        this.browserDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.browserDimension.readFields(dataInput);
        this.statsCommonDimension.readFields(dataInput);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsUserDimension that = (StatsUserDimension) o;
        return Objects.equals(statsCommonDimension, that.statsCommonDimension) &&
                Objects.equals(browserDimension, that.browserDimension);
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public BrowserDimension getBrowserDimension() {
        return browserDimension;
    }

    public void setBrowserDimension(BrowserDimension browserDimension) {
        this.browserDimension = browserDimension;
    }

    @Override
    public BaseDimension getDateDimension() {
        return null;
    }
}
