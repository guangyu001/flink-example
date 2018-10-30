package com.bkjf.flink_example;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bkjf.flink_example.bean.KafkaBinLogEvent;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		String str = "{\"data\":{\"before\":{},\"after\":{\"password\":\"22222222222\",\"cust_id\":\"3\",\"username\":\"1111111\"}},\"dbName\":\"testdb\",\"id\":13,\"operType\":\"INSERT\",\"tableName\":\"test\"}";
		JSONObject obj = (JSONObject) JSON.parse(str);
		System.out.println(obj);
		KafkaBinLogEvent bean = JSONObject.toJavaObject(obj, KafkaBinLogEvent.class);
		System.out.println(bean.getDbName());
		System.out.println(bean.getTableName());
		System.out.println(bean.getOperType());
		Object[] array = bean.getData().getBefore().keySet().toArray();
		for (int i = 0; i < array.length; i++) {
			System.out.println("-------->"+array[i]);
		}
		System.out.println(bean.getData().getAfter().keySet().toArray());
		System.out.println(bean.getData().getBefore());
		
		
		String sqlType = "replace into report.xfl";
		String sql = getSqlStr(sqlType, bean.getData().getAfter());
		System.out.println(sql);
	}
	
	
	
	private static String getSqlStr(String sqlType,Map<String, String> data) {
		StringBuilder sb = new StringBuilder(sqlType);
		Object[] array = data.keySet().toArray();
		sb.append("(");
		StringBuilder csb = new StringBuilder();
		StringBuilder vsb = new StringBuilder();
		for (int i = 0; i < array.length; i++) {
			csb.append(array[i]).append(",");
			vsb.append("'").append(data.get(array[i])).append("',");
		}
		String csbStr = csb.toString();
		String vsbStr = vsb.toString();
		if(!csbStr.endsWith(",") || !vsbStr.endsWith(",")) {
			return null;
		}
		csbStr = csbStr.substring(0,csbStr.length() -1);
		vsbStr = vsbStr.substring(0,vsbStr.length() -1);
		sb.append(csbStr).append(") values(").append(vsbStr).append(")");
		return sb.toString();
	}
}
