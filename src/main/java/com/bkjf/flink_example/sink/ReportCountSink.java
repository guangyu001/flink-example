package com.bkjf.flink_example.sink;

import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import com.bkjf.flink_example.bean.KafkaBinLogEvent;

public class ReportCountSink extends BaseSink{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private List<String> list = new ArrayList<>();
	private List<String> parentnameList = new ArrayList<>();
	private long time = 0;
	private ThreadLocal<SimpleDateFormat> dfThreadLocal = new ThreadLocal<SimpleDateFormat>();

	public ReportCountSink(Map<String, List<String>> columnProcessMap, Map<String, String> columnMap,
			ParameterTool mysqlParameterTool){
		super(columnProcessMap, columnMap, mysqlParameterTool);
		list.add("CONSUMPTION");
		list.add("RECEBANK");
		list.add("RECHARGE");
		parentnameList.add("分账服务");
		parentnameList.add("代收代付类");
		parentnameList.add("履约支付类");
		try {
			time = new SimpleDateFormat("yyyy-MM-dd").parse("2018-01-01").getTime();
		} catch (ParseException e) {
			logger.error("初始化时间时出现异常：", e);
			System.exit(0);
		}
		dfThreadLocal.set(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
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
			if("DELETE".equals(bean.getOperType()) || "UPDATE".equals(bean.getOperType())) {
				return;
			}
			Map<String, String> afterMap = bean.getData().getAfter();
			String createTime = afterMap.get("ad_createtime");
			String ad_accountid = afterMap.get("ad_accountid");
			String ad_visible = afterMap.get("ad_visible");
			String ad_addreduce = afterMap.get("ad_addreduce");
			String ad_entity = afterMap.get("ad_entity");
			if(StringUtils.isEmpty(createTime) || StringUtils.isEmpty(ad_accountid) || StringUtils.isEmpty(ad_visible) || StringUtils.isEmpty(ad_addreduce) || StringUtils.isEmpty(ad_entity)) {
				return;
			}
			if(dfThreadLocal.get().parse(createTime).getTime() < time || ad_accountid.indexOf("bak") > 0 || !ad_visible.equals("0") || !ad_addreduce.equals("1") || !list.contains(ad_entity)) {
				return;
			}
			String querySql = "select mp.parentname,mer.product_id from lift_c.t_core_account t1 left join lft_merchant.m_account mer on t1.aif_cust_id = mer.cust_id and mer.status <> '13' and mer.Acccode <> '0' left join lft_merchant.m_mer_product mp on mp.pid = mer.product_id and mp.type = 1 where t1.aif_accountcode = ''";
			tidbPreparedStatement = tidbConnection.prepareStatement(querySql);
			ResultSet rst = tidbPreparedStatement.executeQuery();
			String parentname = "";
			int product_id = -1;
			if(rst.next()) {
				parentname = rst.getString("parentname");
				String productId = rst.getString("product_id");
				product_id = StringUtils.isEmpty(productId) ? -1 : Integer.parseInt(productId);
			}
			sql = getSqlStr(afterMap, createTime, parentname, product_id);
			tidbPreparedStatement = tidbConnection.prepareStatement(sql);
			tidbPreparedStatement.executeUpdate();
			
		}catch (Exception e) {
			logger.error("执行出现异常,sql = "+sql+" tableName = "+tableName,e);
		}
	}
	
	private String getSqlStr(Map<String, String> afterMap,String createTime,String parentname,int product_id) throws ParseException {
		long all_amt = afterMap.get("ad_amount") == null ? 0 : Long.valueOf(afterMap.get("ad_amount"));
		long all_amt_today = 0;
		if(dfThreadLocal.get().parse(createTime).getTime() >= System.currentTimeMillis()) {
			all_amt_today = all_amt;
		}
		long other_amt = 0;
		if(!parentnameList.contains(parentname) || product_id == 99 || StringUtils.isEmpty(parentname)) {
			other_amt = all_amt;
		}
		long sub_acc_amt = 0;
		if("分账服务".equals(parentname)) {
			sub_acc_amt = all_amt;
		}
		long agent_amt = 0;
		if("代收代付类".equals(parentname)) {
			agent_amt = all_amt;
		}
		long agreement_amt = 0;
		if("履约支付类".equals(parentname) && product_id != 99) {
			agreement_amt = all_amt;
		}
		StringBuilder sb = new StringBuilder();
		sb.append("update lft_report set all_amt = all_amt+").append(all_amt).append(",");
		sb.append("all_amt_today = all_amt_today+").append(all_amt_today).append(",");
		sb.append("other_amt = other_amt+").append(other_amt).append(",");
		sb.append("sub_acc_amt = sub_acc_amt+").append(sub_acc_amt).append(",");
		sb.append("agent_amt = agent_amt+").append(agent_amt).append(",");
		sb.append("agreement_amt = agreement_amt+").append(agreement_amt);
		return sb.toString();
	}

}
