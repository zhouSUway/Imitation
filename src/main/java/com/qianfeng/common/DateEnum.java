package com.qianfeng.common;

public enum  DateEnum {

    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HUOR("huor"),;

    public String type;
    DateEnum(String type){
        this.type=type;
    }

    /**
     * 根据type获取时间枚举
     */

    public DateEnum valueOfType(String type){

        for (DateEnum dateEnum:values()){


            if (this.type.equals(dateEnum.type)){
                return dateEnum;
            }
        }
        throw new RuntimeException("暂时该type不支持该枚举类型"+type);
    }
}
