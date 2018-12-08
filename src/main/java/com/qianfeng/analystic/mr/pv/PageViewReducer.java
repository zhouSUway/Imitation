package com.qianfeng.analystic.mr.pv;


import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.base.KpiDimension;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.analystic.model.dim.value.reduce.MapperOutputValue;
import com.qianfeng.common.KpiTypeEnum;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class PageViewReducer extends Reducer<StatsUserDimension,TimeOutputValue,StatsUserDimension,MapperOutputValue> {

    private IntWritable one = new IntWritable(-1);
    private MapperOutputValue mov = new MapperOutputValue();


    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //map循环过来的value
        int count = 0;
        for (TimeOutputValue timeOutputValue:values){

            //将uuid取出来进行累计
            count++;
        }

        //构建输出的value

        MapWritable mapWritable = new MapWritable();
        mapWritable.put(one,new IntWritable(count));
        this.mov.setMapWritable(mapWritable);

        this.mov.setKpiTypeEnum(KpiTypeEnum.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));

        context.write(key,this.mov);

    }
}
