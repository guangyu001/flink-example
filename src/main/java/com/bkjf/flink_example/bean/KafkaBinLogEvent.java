package com.bkjf.flink_example.bean;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class KafkaBinLogEvent implements Serializable{
	private static final long serialVersionUID = 1L;
	private long id;
	private String dbName;
	private String tableName;
	private String operType;
	private DataBean data;
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getOperType() {
		return operType;
	}
	public void setOperType(String operType) {
		this.operType = operType;
	}
	
	public DataBean getData() {
		return data;
	}
	public void setData(DataBean data) {
		this.data = data;
	}

	public static class DataBean{
		private Map<String, String> before = new HashMap<>();
		private Map<String, String> after = new HashMap<>();
		public Map<String, String> getBefore() {
			return before;
		}
		public void setBefore(Map<String, String> before) {
			this.before = before;
		}
		public Map<String, String> getAfter() {
			return after;
		}
		public void setAfter(Map<String, String> after) {
			this.after = after;
		}
		
	}
}
