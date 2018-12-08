package com.qianfeng.etl.mr.toHdfs;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * /**
 *  *
 *  * @author lyd
 *  *
 *  *
 * create external table if not exists logs(
 * s_time string,
 * en string,
 * ver string,
 * u_ud string,
 * u_mid string,
 * u_sid string,
 * c_time string,
 * language  string,
 * b_iev string,
 * b_rst string,
 * p_url string,
 * p_ref string,
 * tt string,
 * pl string,
 * o_id string,
 * `on` string,
 * cut string,
 * cua string,
 * pt string,
 * ca string,
 * ac string,
 * kv_ string,
 * du string,
 * os string,
 * os_v string,
 * browser string,
 * browser_v string,
 * country string,
 * province string,
 * city string
 * )
 * partitioned by(month String,day string)
 * row format delimited fields terminated by '\001'
 * ;
 *
 * load data inpath '/gpTransform/month=07/day=26' into table logs partition(month=07,day=26);
 *
 */
public class LogDataWritable implements Writable {

        public String s_time;
        public String en;
        public String ver;
        public String u_ud;
        public String u_mid;
        public String u_sid;
        public String c_time;
        public String language = "1";
        public String b_iev;
        public String b_rst;
        public String p_url;
        public String p_ref;
        public String tt;
        public String pl;
        public String oid;
        public String on;
        public String cut;
        public String cua;
        public String pt;
        public String ca;
        public String ac;
        public String kv_;
        public String du;
        public String os;
        public String os_v;
        public String browser;
        public String browser_v;
        public String country;
        public String province;
        public String city;


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.s_time);
        dataOutput.writeUTF(this.en);
        dataOutput.writeUTF(this.ver);
        dataOutput.writeUTF(this.u_ud);
        dataOutput.writeUTF(this.u_mid);
        dataOutput.writeUTF(this.u_sid);
        dataOutput.writeUTF(this.c_time);
        dataOutput.writeUTF(this.language );
        dataOutput.writeUTF(this.b_iev);
        dataOutput.writeUTF(this.b_rst);
        dataOutput.writeUTF(this.p_url);
        dataOutput.writeUTF(this.p_ref);
        dataOutput.writeUTF(this.tt);
        dataOutput.writeUTF(this.pl);
        dataOutput.writeUTF(this.oid);
        dataOutput.writeUTF(this.on);
        dataOutput.writeUTF(this.cut);
        dataOutput.writeUTF(this.cua);
        dataOutput.writeUTF(this.pt);
        dataOutput.writeUTF(this.ca);
        dataOutput.writeUTF(this.ac);
        dataOutput.writeUTF(this.kv_);
        dataOutput.writeUTF(this.du);
        dataOutput.writeUTF(this.os);
        dataOutput.writeUTF(this.os_v);
        dataOutput.writeUTF(this.browser);
        dataOutput.writeUTF(this.browser_v);
        dataOutput.writeUTF(this.country);
        dataOutput.writeUTF(this.province);
        dataOutput.writeUTF(this.city);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.s_time = dataInput.readUTF();
        this.en = dataInput.readUTF();
        this.ver = dataInput.readUTF();
        this.u_ud = dataInput.readUTF();
        this.u_mid = dataInput.readUTF();
        this.u_sid = dataInput.readUTF();
        this.c_time = dataInput.readUTF();
        this.language  = dataInput.readUTF();
        this.b_iev = dataInput.readUTF();
        this.b_rst = dataInput.readUTF();
        this.p_url = dataInput.readUTF();
        this.p_ref = dataInput.readUTF();
        this.tt = dataInput.readUTF();
        this.pl = dataInput.readUTF();
        this.oid = dataInput.readUTF();
        this.on = dataInput.readUTF();
        this.cut = dataInput.readUTF();
        this.cua = dataInput.readUTF();
        this.pt = dataInput.readUTF();
        this.ca = dataInput.readUTF();
        this.ac = dataInput.readUTF();
        this.kv_ = dataInput.readUTF();
        this.du = dataInput.readUTF();
        this.os = dataInput.readUTF();
        this.os_v = dataInput.readUTF();
        this.browser = dataInput.readUTF();
        this.browser_v = dataInput.readUTF();
        this.country = dataInput.readUTF();
        this.province = dataInput.readUTF();
        this.city = dataInput.readUTF();
    }

    public String getS_time() {
        return s_time;
    }

    public void setS_time(String s_time) {
        this.s_time = s_time;
    }

    public String getEn() {
        return en;
    }

    public void setEn(String en) {
        this.en = en;
    }

    public String getVer() {
        return ver;
    }

    public void setVer(String ver) {
        this.ver = ver;
    }

    public String getU_ud() {
        return u_ud;
    }

    public void setU_ud(String u_ud) {
        this.u_ud = u_ud;
    }

    public String getU_mid() {
        return u_mid;
    }

    public void setU_mid(String u_mid) {
        this.u_mid = u_mid;
    }

    public String getU_sid() {
        return u_sid;
    }

    public void setU_sid(String u_sid) {
        this.u_sid = u_sid;
    }

    public String getC_time() {
        return c_time;
    }

    public void setC_time(String c_time) {
        this.c_time = c_time;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getB_iev() {
        return b_iev;
    }

    public void setB_iev(String b_iev) {
        this.b_iev = b_iev;
    }

    public String getB_rst() {
        return b_rst;
    }

    public void setB_rst(String b_rst) {
        this.b_rst = b_rst;
    }

    public String getP_url() {
        return p_url;
    }

    public void setP_url(String p_url) {
        this.p_url = p_url;
    }

    public String getP_ref() {
        return p_ref;
    }

    public void setP_ref(String p_ref) {
        this.p_ref = p_ref;
    }

    public String getTt() {
        return tt;
    }

    public void setTt(String tt) {
        this.tt = tt;
    }

    public String getPl() {
        return pl;
    }

    public void setPl(String pl) {
        this.pl = pl;
    }

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getOn() {
        return on;
    }

    public void setOn(String on) {
        this.on = on;
    }

    public String getCut() {
        return cut;
    }

    public void setCut(String cut) {
        this.cut = cut;
    }

    public String getCua() {
        return cua;
    }

    public void setCua(String cua) {
        this.cua = cua;
    }

    public String getPt() {
        return pt;
    }

    public void setPt(String pt) {
        this.pt = pt;
    }

    public String getCa() {
        return ca;
    }

    public void setCa(String ca) {
        this.ca = ca;
    }

    public String getAc() {
        return ac;
    }

    public void setAc(String ac) {
        this.ac = ac;
    }

    public String getKv_() {
        return kv_;
    }

    public void setKv_(String kv_) {
        this.kv_ = kv_;
    }

    public String getDu() {
        return du;
    }

    public void setDu(String du) {
        this.du = du;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getOs_v() {
        return os_v;
    }

    public void setOs_v(String os_v) {
        this.os_v = os_v;
    }

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    public String getBrowser_v() {
        return browser_v;
    }

    public void setBrowser_v(String browser_v) {
        this.browser_v = browser_v;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "LogDataWritable{" +
                "s_time='" + s_time + '\u0001' +
                ", en='" + en + '\u0001' +
                ", ver='" + ver + '\u0001' +
                ", u_ud='" + u_ud + '\u0001' +
                ", u_mid='" + u_mid + '\u0001' +
                ", u_sid='" + u_sid + '\u0001' +
                ", c_time='" + c_time + '\u0001' +
                ", language='" + language + '\u0001' +
                ", b_iev='" + b_iev + '\u0001' +
                ", b_rst='" + b_rst + '\u0001' +
                ", p_url='" + p_url + '\u0001' +
                ", p_ref='" + p_ref + '\u0001' +
                ", tt='" + tt + '\u0001' +
                ", pl='" + pl + '\u0001' +
                ", oid='" + oid + '\u0001' +
                ", on='" + on + '\u0001' +
                ", cut='" + cut + '\u0001' +
                ", cua='" + cua + '\u0001' +
                ", pt='" + pt + '\u0001' +
                ", ca='" + ca + '\u0001' +
                ", ac='" + ac + '\u0001' +
                ", kv_='" + kv_ + '\u0001' +
                ", du='" + du + '\u0001' +
                ", os='" + os + '\u0001' +
                ", os_v='" + os_v + '\u0001' +
                ", browser='" + browser + '\u0001' +
                ", browser_v='" + browser_v + '\u0001' +
                ", country='" + country + '\u0001' +
                ", province='" + province + '\u0001' +
                ", city='" + city + '\u0001' +
                '}';
    }
}
