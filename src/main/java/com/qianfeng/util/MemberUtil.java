package com.qianfeng.util;

import com.qianfeng.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 查看id是否是新增会员 建议过滤不合法的会员
 */
public class MemberUtil {

    private static final Logger logger = Logger.getLogger(MemberUtil.class);

    private static Map<String, Boolean> cache = new LinkedHashMap<String, Boolean>() {

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return this.size() > 1000;
        }
    };

    /**
     * 检查id是否合法
     * <p>
     * true是合法，false不合法
     */

    public static boolean checkMemberId(String memeberid) {
        String regrx = "^[0-9a-zA-Z].*$";
        if (StringUtils.isNotEmpty(memeberid)) {
            return memeberid.trim().matches(regrx);
        }
        return false;
    }

    /**
     * 是否是一个新增的会员
     * true是新会员 false不是新增会员
     */

    public static boolean isNewMember(String memberId) {

        PreparedStatement pst = null;
        ResultSet rs = null;
        Boolean flag = false;
        Connection conn = null;
        Configuration conf = null;


        try {
            flag = cache.get(memberId);
            if (flag == null) {
                String sql = conf.get(GlobalConstants.PREFIX_TOTAL + "memer_info");
                pst = conn.prepareStatement(sql);
                pst.setString(1, memberId);
                rs = pst.executeQuery();
                if (rs.next()) {
                    flag = false;
                } else {
                    flag = true;
                }
                //添加到cache

                cache.put(memberId, flag);
            }

        } catch (Exception e) {
            logger.warn("新增会员异常", e);
        }
        return flag == null ? false : flag.booleanValue();
    }
}