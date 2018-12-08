package com.qianfeng.analystic.model.dim.base;

import com.qianfeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 定义浏览器维度
 */
public class BrowserDimension extends BaseDimension {

    private int id;
    private String browserName;
    private String browerVersion;

    public  BrowserDimension(){}

    public BrowserDimension(String browserName, String browerVersion) {
        this.browserName = browserName;
        this.browerVersion = browerVersion;
    }

    public BrowserDimension(int id, String browserName, String browerVersion) {
        this(browserName,browerVersion);
        this.id = id;
    }

    /**
     * 获取当前类的实例
     * @param browserName
     * @param browerVersion
     * @return
     */
    public static BrowserDimension newInstance(String browserName,String browerVersion){
        BrowserDimension browserDimension = new BrowserDimension();
        browserDimension.browserName = browserName;
        browserDimension.browerVersion=browerVersion;
        return browserDimension;
    }

    /**
     * 构建浏览器维度的集合对象
     * @param
     * @return
     */
    public static List<BrowserDimension> buildBrower(String browserName,String browerVersion){
        List<BrowserDimension> lists = new ArrayList<BrowserDimension>();

        if(StringUtils.isEmpty(browserName)){
            browserName=browerVersion=GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(browerVersion)){
            browerVersion=GlobalConstants.DEFAULT_VALUE;
        }
        lists.add(newInstance(browserName,browerVersion));
        lists.add(newInstance(browserName,GlobalConstants.ALL_OF_VALUE));
        return lists;
    }

    //重写比较器
    @Override
    public int compareTo(BaseDimension o) {

        if (this==o){
            return 0;
        }
        BrowserDimension other = (BrowserDimension) o;
        int tmp = this.id-other.id;
        if (tmp!=0){
            return tmp;
        }
        tmp = this.browserName.compareTo(other.browserName);
        if (tmp!=0){
            return tmp;
        }
        tmp = this.browerVersion.compareTo(other.browerVersion);
        return tmp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        /**
         * 序列化
         */
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(browserName);
        dataOutput.writeUTF(browerVersion);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        /**
         * 反序列化
         */
        this.id=dataInput.readInt();
        this.browserName=dataInput.readUTF();
        this.browerVersion=dataInput.readUTF();
    }

    /**
     * 重写equals/hashcode与get/set
     */
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBrowserName() {
        return browserName;
    }

    public void setBrowserName(String browserName) {
        this.browserName = browserName;
    }

    public String getBrowerVersion() {
        return browerVersion;
    }

    public void setBrowerVersion(String browerVersion) {
        this.browerVersion = browerVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrowserDimension that = (BrowserDimension) o;
        return id == that.id &&
                Objects.equals(browserName, that.browserName) &&
                Objects.equals(browerVersion, that.browerVersion);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, browserName, browerVersion);
    }
}
