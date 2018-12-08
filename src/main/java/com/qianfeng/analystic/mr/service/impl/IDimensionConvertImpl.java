package com.qianfeng.analystic.mr.service.impl;

import com.qianfeng.analystic.model.dim.base.*;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import com.qianfeng.util.JdbcUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 操作维度表的实现接口
 */
public class IDimensionConvertImpl implements IDimensionConvert {
    public static Logger logger = Logger.getLogger(IDimensionConvertImpl.class);

    //对维度的id进行缓存

    private Map<String,Integer> cache =new LinkedHashMap<String, Integer>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size()>1000;
        }
    };

    /**
     * 获取维度的id
     * 查询缓存中是否存在对应的维度，如果有则取出
     * 1.先用维度去查询数据库，如果有则返回存在的维度id
     * 2.如果不存在则先插入，后取出
     * @param dimension
     * @return
     * @throws IOException
     * @throws SQLException
     */
    @Override
    public int getDimensionInterfaceById(BaseDimension dimension) throws IOException, SQLException {

        String cachekey =  this.buildCache(dimension);
        if (this.cache.containsKey(cachekey)){
            return this.cache.get(cachekey);
        }

        //如果缓存中没有则需要去查询数据库中的数据

        String sqls [] = null;

        if (dimension instanceof BrowserDimension){
            sqls = this.buildBrowerSqls();
        }else  if (dimension instanceof DateDimension){
            sqls = this.buildDateSqls();
        }else  if (dimension instanceof PlatformDimension){
            sqls = this.buildPlatformSqls();
        }else  if (dimension instanceof KpiDimension){
            sqls = this.buildKpiSqls();
        }

        Connection conn = JdbcUtil.getConn();
        int id =-1;
        synchronized (this){
            id = this.execute(sqls,dimension,conn);
        }

        //将取到的id缓存到cache中

        this.cache.put(cachekey,id);
        return id;
    }

    /**
     * 真正的查询语句 构建 sql语句
     * @return
     */

    private String[] buildBrowerSqls() {
        String selected = "select `id` from `dimension_browser` where  `browser_name` = ? and `browser_version` = ?";
        String installed ="insert into `dimension_browser`(`browser_name`,`browser_version`) values(?,?)";
        return new String[]{selected,installed};
    }


    private String[] buildDateSqls() {

        String selected = "select `id` from `dimension_date` where `year`  = ? and `season` = ? and `month` = ? and `week` = ? and `day` = ? and `calendar` = ? and `type` = ?";
        String installed = "insert into `dimension_date`(`year`,`season`, `month`,`week`,`day`,`calendar`,`type`) values(?,?,?,?,?,?,?)";
        return  new String[]{selected,installed};
    }


    private String[] buildPlatformSqls() {
        String selected= "select `id` from `dimension_platform` where `platform_name` = ?";
        String installed ="insert into `dimension_platform` (`platform_name`) values(?)";
        return new String[]{selected,installed};
    }

    private String[] buildKpiSqls() {
        String selected = "select `id` from `dimension_kpi` where `kpi_name` = ?";
        String installed = "insert into `dimension_kpi` (`kpi_name`) values(?)";
        return new String[]{selected,installed};
    }

    /**
     * 执行SQL语句的程序
     *
     */


    public int execute(String[] sqls, BaseDimension dimension, Connection conn){
        PreparedStatement pst = null;
        ResultSet rs = null;

        //1.首先执行查询语句,在插入

        try {
            pst = conn.prepareStatement(sqls[0]);
            this.handleSql(dimension,pst);
            pst.executeQuery();
            rs = pst.getGeneratedKeys();

            if (rs.next()){
                return  rs.getInt(1);
            }

            //查询不到在插入
            pst = conn.prepareStatement(sqls[1],Statement.RETURN_GENERATED_KEYS);
            this.handleSql(dimension,pst);
            pst.executeUpdate();
            rs = pst.getGeneratedKeys();
            if (rs.next()){
                return  rs.getInt(1);
            }
        } catch (SQLException e) {
          logger.warn("SQL语句执行异常",e);
        }finally {
            try {
                JdbcUtil.getClosed(conn,pst,rs);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        throw new RuntimeException("查询与插入都出现异常");
    }

    /**
     * 设置参数
     * sql语句的查询过程
     * 及维度与数据库中的数据对应上
     * 语句与维度
     * @param dimension
     * @param pst
     */
    private void handleSql(BaseDimension dimension, PreparedStatement pst) {

        try {
            int i=0;
            if (dimension instanceof BrowserDimension){

                BrowserDimension browserDimension = (BrowserDimension) dimension;
                pst.setString(++i,browserDimension.getBrowserName());
                pst.setString(++i,browserDimension.getBrowerVersion());

            }else if (dimension instanceof PlatformDimension){

                PlatformDimension platformDimension = (PlatformDimension) dimension;

                pst.setString(++i,platformDimension.getPlatformName());

            }else  if (dimension instanceof  DateDimension){

                DateDimension dateDimension = (DateDimension)  dimension;

                pst.setInt(++i,dateDimension.getYear());
                pst.setInt(++i,dateDimension.getSeason());
                pst.setInt(++i,dateDimension.getMonth());
                pst.setInt(++i,dateDimension.getWeek());
                pst.setInt(++i,dateDimension.getDay());
                pst.setDate(++i,new Date(dateDimension.getCalender().getTime()));
                pst.setString(++i,dateDimension.getType());

            }else if (dimension instanceof KpiDimension){

                KpiDimension kpiDimension = (KpiDimension) dimension;

                pst.setString(++i,kpiDimension.getKpiName());
            }

        } catch (Exception e) {

            logger.warn("设置参数异常",e);
        }


    }

    /**
     * 获取对相应的维度  及接口对接
     * @param dimension
     * @return
     */
    private String buildCache(BaseDimension dimension) {

        StringBuffer sb = new StringBuffer();

        if (dimension instanceof BrowserDimension){
            sb.append("brower_");
            BrowserDimension browserDimension = (BrowserDimension) dimension;
            sb.append(browserDimension.getBrowserName());
            sb.append(browserDimension.getBrowerVersion());

        }else if (dimension instanceof DateDimension){
            sb.append("date_");
            DateDimension dateDimension = (DateDimension) dimension;
            sb.append(dateDimension.getYear());
            sb.append(dateDimension.getSeason());
            sb.append(dateDimension.getMonth());
            sb.append(dateDimension.getWeek());
            sb.append(dateDimension.getDay());
            sb.append(dateDimension.getCalender());
            sb.append(dateDimension.getType());

        }else if(dimension instanceof PlatformDimension){
            sb.append("platform_");
            PlatformDimension platformDimension = (PlatformDimension) dimension;
            sb.append(platformDimension.getPlatformName());

        }else if(dimension instanceof KpiDimension){
            sb.append("kpi_");
            KpiDimension kpiDimension = (KpiDimension) dimension;
            sb.append(kpiDimension.getKpiName());
        }

        return sb.length() == 0 ? null:sb.toString();

    }
}
