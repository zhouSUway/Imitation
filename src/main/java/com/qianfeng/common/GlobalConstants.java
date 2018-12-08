package com.qianfeng.common;

import javax.xml.transform.sax.SAXTransformerFactory;

/**
 * jdbc的全局变量属性
 */
public class GlobalConstants {

    public static final String DRIVER = "com.mysql.jdbc.Driver";
    public static final String URL = "jdbc:mysql://192.168.243.130:3306/result";
    public static final String ROOT = "root";
    public static final String PASSWORDS = "root";

    public static final String DEFAULT_VALUE = "unknown";
    public static final String ALL_OF_VALUE = "all";


    public static final String RUNNING_DATE = "running_date";
    public static final String PREFIX_TOTAL = "total_";

    public static final String PREFIX_OUTPUT = "output_";
    public static final int NUM_OF_BATCH = 50;
    public static final long DAY_OF_MILISECONDS = 86400000L;//24*60*60*1000
}
