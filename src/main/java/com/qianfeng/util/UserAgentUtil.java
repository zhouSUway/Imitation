package com.qianfeng.util;


import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 浏览器对象解析
 */
public class UserAgentUtil {

    public static final Logger logger = Logger.getLogger(UserAgentUtil.class);


    UserAgentInfo Info = new UserAgentInfo();

    //获取usaparser对象

    private static UASparser uaSparser = null;

    static {
        try {
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            logger.warn("获取usaParser对象失败",e);
        }
    }


    public UserAgentInfo parserUserAgent(String userAgent){

        if (StringUtils.isEmpty(userAgent)){
            return null;
        }
        //使用usaparser获取对象代理

        try {
            cz.mallat.uasparser.UserAgentInfo uas = uaSparser.parse(userAgent);

            if (uas!=null){
                //设置为info信息代理
                Info.setBrowserName(uas.getUaFamily());
                Info.setBrowserVersion(uas.getBrowserVersionInfo());
                Info.setOsName(uas.getOsFamily());
                Info.setOsVersion(uas.getOsName());
            }
        } catch (IOException e) {
            logger.warn("uas解析失败",e);
        }

        return Info;
    }

    /**
     * 封装浏览器的基本信息
     */


    public static class UserAgentInfo{

        private String browserName;
        private String browserVersion;
        private String osName;
        private String osVersion;

        public String getBrowserName() {
            return browserName;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    "browserName='" + browserName + '\'' +
                    ", browserVersion='" + browserVersion + '\'' +
                    ", osName='" + osName + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    '}';
        }
    }




}
