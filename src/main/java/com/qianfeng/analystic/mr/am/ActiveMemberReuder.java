package com.qianfeng.analystic.mr.am;

import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.analystic.model.dim.value.reduce.MapperOutputValue;
import com.qianfeng.common.KpiTypeEnum;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ActiveMemberReuder extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension,MapperOutputValue> {

    private Set<String> unique = new HashSet<String>();
    private MapperOutputValue value = new MapperOutputValue();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {

        //清空unique
        this.unique.clear();

        //循环map 阶段传过来的value
        for (TimeOutputValue tv:values){
            //将uuid 取出来的添加到set中
            this.unique.add(tv.getId());
        }

        //构建输出的value

        MapWritable mapWritable = new MapWritable();
        mapWritable.put(new IntWritable(-1),new IntWritable(this.unique.size()));

        this.value.setKpiTypeEnum(KpiTypeEnum.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));

        //输出即可
        context.write(key,this.value);

    }
}
