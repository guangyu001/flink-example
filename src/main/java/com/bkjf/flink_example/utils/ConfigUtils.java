package com.bkjf.flink_example.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

public class ConfigUtils {
	private static final String COLUMN_PROCESS = "columnprocess";
	private static final String COLUMN_MAP = "columnmap";
	
	public static Map<String, List<String>> getColumnProcessConfig(ParameterTool sysParameterTool) throws IOException {
		Map<String, List<String>> configMap = new HashMap<>();
		Set<String> nameSet = sysParameterTool.getProperties().stringPropertyNames();
		for(String name : nameSet) {
			if(!name.startsWith(COLUMN_PROCESS)) {
				continue;
			}
			String csStr = sysParameterTool.get(name);
			if(StringUtils.isEmpty(csStr)) {
				continue;
			}
			String[] cs = csStr.split(",");
			List<String> list = new ArrayList<>();
			for(String c : cs) {
				list.add(c);
			}
			name = name.replace(COLUMN_PROCESS+".", "");
			configMap.put(name, list);
		}
		return configMap;
	}
	
	public static Map<String, String> getColumnMapConfig(ParameterTool sysParameterTool) throws IOException {
		Map<String, String> configMap = new HashMap<>();
		Set<String> nameSet = sysParameterTool.getProperties().stringPropertyNames();
		for(String name : nameSet) {
			if(!name.startsWith(COLUMN_MAP)) {
				continue;
			}
			String csStr = sysParameterTool.get(name);
			if(StringUtils.isEmpty(csStr)) {
				continue;
			}
			name = name.replace(COLUMN_MAP+".", "");
			configMap.put(name, csStr);
		}
		return configMap;
	}
}
