package com.qianfeng.analystic.mr.session;

import com.qianfeng.analystic.model.dim.StatsUserDimension;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.analystic.model.dim.value.reduce.MapperOutputValue;
import com.qianfeng.common.GlobalConstants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class SessionReducer extends Reducer<StatsUserDimension,TimeOutputValue,StatsUserDimension,TimeOutputValue> {
private Map<String, List<Long>> map = new HashMap<String, List<Long>>();


    private MapperOutputValue value = new MapperOutputValue();


    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {

        //清空map
        this.map.clear();
        //循环map阶段传过来的value
        for (TimeOutputValue tv:values){
            String sessionId = tv.getId();
            Long serverTime = tv.getTime();
            //存储时间
            if (map.containsKey(tv.getId())){
                List<Long> li = map.get(sessionId);
                li.add(serverTime);
                map.put(sessionId,li);
            }else {
                List<Long> li = new ArrayList<>();
                li.add(serverTime);
                map.put(sessionId,li);
            }
        }

        //构建输出的value
        MapWritable mapWritable = new MapWritable();
        mapWritable.put(new IntWritable(-1),new IntWritable(this.map.size()));

        //session的时长
        int sessionLength =0;
        for (Map.Entry<String,List<Long>> en:map.entrySet()){
            if (en.getValue().size()>=2){
                Collections.sort(en.getValue());
                sessionLength+=(en.getValue().get(en.getValue().size()-1)-en.getValue().get(0));
                System.out.println(en.getKey()+"aaaaa"+sessionLength);
            }
        }

        System.out.println("aaaa"+sessionLength);

        if (sessionLength>0&&sessionLength<=GlobalConstants.DAY_OF_MILISECONDS){
            //不足一秒算一秒
            if (sessionLength%1000==0){
                sessionLength=sessionLength/1000;
            }
        }


    }
}
