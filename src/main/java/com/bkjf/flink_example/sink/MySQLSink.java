package com.bkjf.flink_example.sink;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import com.bkjf.flink_example.bean.KafkaBinLogEvent;


public class MySQLSink extends BaseSink{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public MySQLSink(Map<String, List<String>> columnProcessMap, Map<String, String> columnMap,
			ParameterTool mysqlParameterTool) {
		super(columnProcessMap, columnMap, mysqlParameterTool);
	}
	
	@Override
	public void invoke(KafkaBinLogEvent bean) throws Exception {
		String tableName = bean.getTableName();
		List<String> columnList = columnProcessMap.get(tableName);
		if(columnList == null || columnList.size() == 0) {
			return;
		}
		String sql = "";
		try {
			if("INSERT".equals(bean.getOperType()) || "UPDATE".equals(bean.getOperType())) {
				String sqlType = "INSERT INTO "+tidbTableName;
				sql = getSqlStr(bean.getDbName(),tableName,columnList,sqlType,bean.getData().getAfter());
				if(StringUtils.isEmpty(sql)) {
					return;
				}
				logger.info("往聚合db中插入数据，sql = "+sql);
				tidbPreparedStatement = tidbConnection.prepareStatement(sql);
				tidbPreparedStatement.executeUpdate();
			}else if("DELETE".equals(bean.getOperType())) {
				//TODO
			}
		}catch (Exception e) {
			logger.error("执行出现异常,sql = "+sql+" tableName = "+tableName,e);
		}
	}
	
	private String getSqlStr(String dbName,String tableName,List<String> columnList,String sqlPrefix,Map<String, String> data) {
		StringBuilder sb = new StringBuilder(sqlPrefix);
		Object[] array = data.keySet().toArray();
		sb.append("(");
		StringBuilder csb = new StringBuilder();
		StringBuilder vsb = new StringBuilder();
		StringBuilder updateSb = new StringBuilder();
		for (int i = 0; i < array.length; i++) {
			String key = String.valueOf(array[i]);
			if(!columnList.contains(key)) {
				continue;
			}
			if("order_no".equals(key) && StringUtils.isEmpty(data.get(key))) {
				logger.error("order_no为空，dbName = "+dbName+"  tableName = "+tableName);
				continue;
			}
			String cloumn = columnMap.get(tableName+"_"+key) == null ? key : columnMap.get(tableName+"_"+key);
			String value = getValue(data.get(key));
			csb.append(cloumn).append(",");
			vsb.append(value).append(",");
			updateSb.append(cloumn).append(" = ").append(value).append(",");
		}
		String csbStr = csb.toString();
		String vsbStr = vsb.toString();
		String updateSbStr = updateSb.toString();
		if(!csbStr.endsWith(",") || !vsbStr.endsWith(",") || !updateSbStr.endsWith(",")) {
			return null;
		}
		csbStr = csbStr.substring(0,csbStr.length() -1);
		vsbStr = vsbStr.substring(0,vsbStr.length() -1);
		updateSbStr = updateSbStr.substring(0,updateSbStr.length() -1);
		sb.append(csbStr).append(") VALUES(").append(vsbStr).append(")");
		sb.append("ON DUPLICATE KEY UPDATE ").append(updateSbStr);
		return sb.toString();
	}
	
	private String getValue(String str) {
		if(StringUtils.isEmpty(str)) {
			return null;
		}
		if(str.endsWith("'")) {
			return "'"+str;
		}
		if("null".equals(str)) {
			return null;
		}
		return "'"+str+"'";
	}
}
