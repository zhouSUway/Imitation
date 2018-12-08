package com.qianfeng.util;

import com.qianfeng.common.DateEnum;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.SimpleTimeZone;

import static org.apache.commons.configuration.DataConfiguration.DEFAULT_DATE_FORMAT;

public class TimeUtil {

    private static final Logger logger = Logger.getLogger(TimeUtil.class);


    public static  int getDateInfo(long time, DateEnum type){

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);

        if (type.equals(DateEnum.YEAR)){
            return calendar.get(Calendar.YEAR);
        }
        if (type.equals(DateEnum.SEASON)){
            int month = calendar.get(Calendar.MONTH)+1;
            return (month+2)/3;
        }
        if (type.equals(DateEnum.MONTH)){
            return calendar.get(Calendar.MONTH)+1;
        }
        if (type.equals(DateEnum.WEEK)){
            return calendar.get(Calendar.DAY_OF_WEEK);
        }
        if (type.equals(DateEnum.DAY)){
            return calendar.get(Calendar.DAY_OF_MONTH);
        }
        if (type.equals(DateEnum.HUOR)){
            return calendar.get(Calendar.HOUR_OF_DAY);
        }

        throw  new RuntimeException("该type不支持该类型");
    }

    /**
     * 获取昨天的信息
     */
    public static String  getYesterdayDate(String time){

        SimpleDateFormat sdf = new SimpleDateFormat(time);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR,1);
        return sdf.format(calendar.getTime());
    }

    /**
     * 根据时间戳的所在周的的第一天
     */

    public static long getFirstDayOfWeek(long time){

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.DAY_OF_WEEK,1);
        calendar.set(Calendar.HOUR_OF_DAY,0);
        calendar.set(Calendar.MINUTE,0);
        calendar.set(Calendar.SECOND,0);
        calendar.set(Calendar.MILLISECOND,0);
        return calendar.getTimeInMillis();
    }


    /**
     * 将字符串的日期转换成时间戳, 默认的格式
     * @param date
     * @return
     */
    public static long parserString2Long(String date){
        return parserString2Long(date,DEFAULT_DATE_FORMAT);
    }

    /**
     * 将字符串的日期转换成时间戳 2018-07-26  15127398848923
     * @param date
     * @return
     */
    public static long parserString2Long(String date,String parttern){
        SimpleDateFormat sdf = new SimpleDateFormat(parttern);
        Date dt = null;
        try {
            dt = sdf.parse(date);

        } catch (ParseException e) {
            logger.warn("解析字符串的date为时间戳异常",e);
        }
        return dt == null ? 0 : dt.getTime();
    }


    /**
     * 将时间戳转换成日期, 默认的格式
     * @param time
     * @return
     */
    public static String parserLong2String(long time){
        return parserLong2String(time,DEFAULT_DATE_FORMAT);
    }

    /**
     * 将时间戳转换为指定格式的日期
     * @param time
     * @param format
     * @return
     */
    public static String parserLong2String(long time,String format){
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        return new SimpleDateFormat(format).format(calendar.getTime());
    }

}
