package com.qianfeng.analystic.mr.nu;

import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.analystic.model.dim.value.reduce.MapperOutputValue;
import com.qianfeng.common.KpiTypeEnum;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 新增用户的reducer
 */
public class NewUserReducer extends Reducer<StatsUserDimension,TimeOutputValue,
        StatsUserDimension,MapperOutputValue> {

    private Set<String> unique = new HashSet<String>();
    private MapperOutputValue mov = new MapperOutputValue();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {


        //清空unique
        this.unique.clear();
        for (TimeOutputValue tov :values){
            //把uuid取出来存储到Set中
            this.unique.add(tov.getId());
        }

        MapWritable mapperOutputValue = new MapWritable();
        mapperOutputValue.put(new IntWritable(-1),new IntWritable(this.unique.size()));

        this.mov.setMapWritable(mapperOutputValue);
        this.mov.setKpiTypeEnum(KpiTypeEnum.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));


        //输出即可

        context.write(key,this.mov);
    }
}
