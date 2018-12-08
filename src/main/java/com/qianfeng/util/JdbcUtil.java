package com.qianfeng.util;


import com.qianfeng.common.GlobalConstants;

import java.sql.*;

/*
数据库连接工具类
 */
public class JdbcUtil {
    /**
     *
     * 静态类出出力数据
     */
    static {
        try {
            Class.forName(GlobalConstants.DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 数据连接工具类
     * @return
     */
    public static Connection getConn(){

        Connection conn = null;

        try {
            conn = DriverManager.getConnection(GlobalConstants.URL,GlobalConstants.ROOT,GlobalConstants.PASSWORDS);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conn;
    }

    public static void getClosed(Connection conn, PreparedStatement pst, ResultSet rs)throws Exception{


        if (conn!=null){
            conn.close();
        }
        if (pst!=null){
            pst.close();
        }
        if (rs!=null){
            rs.close();
        }

    }

    public static void main(String[] args) {
        System.out.println(getConn());
    }

}
