package com.qianfeng.analystic.model.dim.base;

import com.qianfeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PlatformDimension extends BaseDimension {

    private int id;
    private String platformName;

    public PlatformDimension(){}

    public PlatformDimension(String platformName) {
        this.platformName = platformName;
    }

    public PlatformDimension(int id, String platformName) {
        this.id = id;
        this.platformName = platformName;
    }

    /**
     * 构建平台维度的集合对象
     * @param platformName
     * @return
     */

    public static List<PlatformDimension> buildPlatform(String platformName){

        if (StringUtils.isEmpty(platformName)){
            platformName = GlobalConstants.DEFAULT_VALUE;
        }
        List<PlatformDimension> list = new ArrayList<PlatformDimension>();

        list.add(new PlatformDimension(platformName));
        list.add(new PlatformDimension(GlobalConstants.DEFAULT_VALUE));

        return list;
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
        PlatformDimension other = (PlatformDimension) o;
        int tmp = this.id-other.id;
        if (tmp!=0){
            return tmp;
        }
        tmp = this.platformName.compareTo(other.platformName);
        return tmp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        /**
         * 序列化数据
         */
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.platformName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        /**
         * 反序列化数据
         */

        this.id = dataInput.readInt();
        this.platformName = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlatformDimension that = (PlatformDimension) o;
        return id == that.id &&
                Objects.equals(platformName, that.platformName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, platformName);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }
}
