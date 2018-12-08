package com.qianfeng.common;

/**
 * kpi的枚举lei
 */
public enum  KpiTypeEnum {

    NEW_USER("new_user"),
    BROWSER_NEW_USER("browser_new_user"),
    ACTIVE_USER("active_user"),
    BROWSER_ACTIVE_USER("browser_active_user"),
    ACTIVE_MEMBER("active_member"),
    BROWSER_ACTIVE_MEMBER("browser_active_member"),
    NEW_MEMBER("new_member"),
    BROWSER_NEW_MEMBER("browser_new_member"),
    MEMBER_INFO("member_info"),
    SESSION("session"),
    BROWSER_SESSION("browser_session"),
    HOURLY_ACTIVE_USER("hourly_active_user"),
    HOURLY_SESSION("hourly_session"),
    PAGEVIEW("pageview"),
    LOCAL("local"),
    ;

    public String kpiName;

    KpiTypeEnum (String kpiName){};

    public static KpiTypeEnum valueOfType(String kpiName){
        for (KpiTypeEnum kpiTypeEnum:values()){
            if(kpiName.equals(kpiTypeEnum.kpiName)){

                return kpiTypeEnum;
            }

        }
        throw new RuntimeException("kpiName不支持该枚举类型");
    }

}
