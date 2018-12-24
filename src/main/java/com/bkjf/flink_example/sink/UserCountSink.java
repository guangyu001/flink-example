package com.bkjf.flink_example.sink;

import com.bkjf.flink_example.bean.KafkaBinLogEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @Author: guang yu
 * @Description:
 * @Date: Create in 18:33
 * @Modified By: your name
 */

public class UserCountSink extends BaseSink {

    public UserCountSink(Map<String, List<String>> columnProcessMap, Map<String, String> columnMap, ParameterTool mysqlParameterTool) {
        super(columnProcessMap, columnMap, mysqlParameterTool);
    }

    @Override
    public void invoke(KafkaBinLogEvent bean) throws Exception {
        String sql = "";
        try {
            sql = getSqlStr(tidbTableName);
            System.out.println(sql);
            String delSql = getDelSql(tidbTableName);
            logger.info("删除t-1数据，sql = " + sql);
            tidbPreparedStatement = tidbConnection.prepareStatement(delSql);
            tidbPreparedStatement.execute();
            logger.info("更新数据，sql = " + sql);
            tidbPreparedStatement = tidbConnection.prepareStatement(sql);
            tidbPreparedStatement.execute();
            System.out.println("处理完毕");

        } catch (Exception e) {
            String tableName = bean.getTableName();
            System.out.println("异常");
            logger.error("执行出现异常,sql = " + sql + " tableName = " + tableName, e);
        }
    }

    private String getSqlStr(String actualTableName) throws IOException {

        //FileReader流读取SQL

        /*actualTableName = "api_service.lft_report_real_user_count";
        String[] array = actualTableName.split("\\.");
        System.out.println(actualTableName);
        System.out.println(array.length);
        String tableName = array[1];
        System.out.println(tableName);
        String sql = readSQL(tableName);
        System.out.println(sql);
        System.out.println("正在操作实时报表:	" + actualTableName);
        return sql.toString();*/
        System.out.println("INSERT 操作实时报表报表    "+actualTableName);
        StringBuilder sb = new StringBuilder();
        sb.append("insert into api_service.lft_report_real_all_user_cnt\n" +
                "(user_count,etl_time)\n" +
                "select count(1),now() from LIFT_PS.T_INDIVIDUAL_INFO");
        return sb.toString();
    }

    private String getDelSql(String actualTableName){
        System.out.println("DELETE 操作实时报表报表    "+actualTableName);
        StringBuilder sb = new StringBuilder();
        sb.append("delete from api_service.lft_report_real_all_user_cnt where etl_time<current_date();");
        return sb.toString();
    }

    /**
     * fileReader流读取简单SQL语句
     * @param tableName
     * @return
     */
    private static String readSQL(String tableName) {
        StringBuffer str = new StringBuffer("");
        String fileName = MySQLSink.class.getClassLoader().getResource("/sql/"+tableName+".txt").getPath();
        //System.out.println(fileName);
        File file = new File(fileName);
        try {
            FileReader fr = new FileReader(file);
            int ch = 0;
            while ((ch = fr.read()) != -1) {
                System.out.print((char) ch);
            }
            fr.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("File reader出错");
        }
        return str.toString();
    }

    private String getValue(String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        }
        if (str.endsWith("'")) {
            return "'" + str;
        }
        if ("null".equals(str)) {
            return null;
        }
        return "'" + str + "'";
    }
    @Override
    public void close() throws Exception {
        logger.info("db 连接关闭");
        if (tidbPreparedStatement != null) {
            tidbPreparedStatement.close();
        }
        if (tidbConnection != null) {
            tidbConnection.close();
        }
    }




}
