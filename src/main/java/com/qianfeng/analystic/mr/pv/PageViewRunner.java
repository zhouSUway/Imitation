package com.qianfeng.analystic.mr.pv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import javax.lang.model.SourceVersion;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

/**
 * 活跃用户的runner类
 */
public class PageViewRunner implements Tool {
    private static final Logger logger = Logger.getLogger(PageViewRunner.class);

    private Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new PageViewRunner(),args);

        } catch (Exception e) {
            logger.info("运行指标失败");
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }

    @Override
    public void setConf(Configuration conf) {

        this.conf.addResource("query-mapping.xml");
        this.conf.addResource("output-writter.xml");
        this.conf=HBaseConfiguration.create(this.conf);

    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
