package com.qianfeng.analystic.hive.en;


import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.mr.service.impl.IDimensionConvertImpl;
import com.qianfeng.common.DateEnum;
import com.qianfeng.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;

public class DataDimensionUDF extends com.aliyun.odps.udf.UDF {

    IDimensionConvertImpl convert = new IDimensionConvertImpl();

    public DataDimensionUDF() {
        super();
    }

    public int evaluate(Text str, int i) {

        String dt = str.toString();

        if (StringUtils.isEmpty(dt)) {

            dt = TimeUtil.getYesterdayDate();
        }

        DateDimension ds = DateDimension.buildDate(TimeUtil.parserString2Long(dt), DateEnum.DAY);
        int id = -1;

        int d = -1;
        try {

            id = convert.getDimensionInterfaceById(ds);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return id + i;

    }

    public static void main(String[] args) {
        System.out.println(new DataDimensionUDF().evaluate(new Text("2018-12-01"),2));
    }
}
