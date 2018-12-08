package com.qianfeng.util;

import com.qianfeng.common.EventConstant;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Map;

/**
 * 将采集的日志一行行解析成key-value对，便于存储
 *
 */
public class LogUtil {

    public static  final Logger logger = Logger.getLogger(LogUtil.class);

    /**
     *
     * 192.168.216.111^A1532576375.965^A192.168.216.111^A
     *  /index.html?ver=1.0&u_mid=123&en=e_cr&c_time=1532576375614&
     *  ip:192.168.216.111
     *  s_time:1532576375.965
     *   ver:1.0
     */

    public static Map praserLog(String logs){
        //定义一个Map

        Map<String,String> info = new ConcurrentHashMap<String,String>();
        if (StringUtils.isEmpty(logs)){

            String[] fields = logs.split(EventConstant.COLUMN_SEPARTOR);
            if (fields.length==4){
                info.put(EventConstant.EVENT_COLUMN_NAME_IP,fields[0]);
                info.put(EventConstant.EVENT_COLUMN_NAME_SERVER_TIME,fields[1].replace("\\.",""));
                //判断是否有参数列表

                int index = fields[3].indexOf("?");
                if (index>0){
                    //获取参数列表
                    String parms = fields[3].substring(index + 1);

                    //解析URL的参数
                    handleParams(info,parms);
                    //解析ip地址
                    handleIp(info);
                    //解析useragent
                    handleUserAgent(info);
                }
            }
        }

        return info;
    }

    /**
     * 解析ip地址
     * @param info
     */
    private static void handleIp(Map<String,String> info) {
        if (info.containsKey(EventConstant.EVENT_COLUMN_NAME_IP)){
            IpUtil.RegionInfo regions = IpUtil.parserIp(info.get(EventConstant.EVENT_COLUMN_NAME_IP));
            if (regions!=null){
                info.put(EventConstant.EVENT_COLUMN_NAME_COUNTRY,regions.getCountry());
                info.put(EventConstant.EVENT_COLUMN_NAME_PROVINCE,regions.getProvince());
                info.put(EventConstant.EVENT_COLUMN_NAME_CITY,regions.getCity());
            }
        }
    }

    /**
     * 解析useragent 及浏览器用户型号
     * @param info
     */
    private static void handleUserAgent(Map<String,String> info) {

        if (info.containsKey(EventConstant.EVENT_COLUMN_NAME_USERAGENT)){
            UserAgentUtil.UserAgentInfo userInfo = new UserAgentUtil().parserUserAgent(info.get(EventConstant.EVENT_COLUMN_NAME_USERAGENT));
            if (userInfo!=null){
                info.put(userInfo.getBrowserName(),EventConstant.EVENT_COLUMN_NAME_BROWSER_NAME);
                info.put(userInfo.getBrowserVersion(),EventConstant.EVENT_COLUMN_NAME_BROWSER_VERSION);
                info.put(userInfo.getOsName(),EventConstant.EVENT_COLUMN_NAME_OS_NAME);
                info.put(userInfo.getOsVersion(),EventConstant.EVENT_COLUMN_NAME_OS_VERSION);
            }
        }
    }

    /**
     * 解析url路径的信息 将参数列表以key-value的形式保存在info
     * @param info
     * @param parms
     */
    private static void handleParams(Map<String,String> info, String parms) {

        if (StringUtils.isNotEmpty(parms)){
            String[] lines = parms.split("&");
            try {
                for (String kvs:lines){
                    String[] kv = kvs.split("=");
                    String k = kv[0];
                    String v = URLDecoder.decode(kv[1],"utf-8");
                    if (StringUtils.isNotEmpty(k)){
                        info.put(k,v);
                    }
                }

            }catch (Exception e){
                logger.warn("参数列表异常",e);
            }
        }
    }


}
