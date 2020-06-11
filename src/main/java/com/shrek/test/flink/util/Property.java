package com.shrek.test.flink.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

@Slf4j
public class Property {

	private final static String CONF_NAME = "config.properties";

	private static Properties contextProperties;

	static {
		InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONF_NAME);
		contextProperties = new Properties();
		try {
			InputStreamReader inputStreamReader = new InputStreamReader(in, "UTF-8");
			contextProperties.load(inputStreamReader);
		} catch (IOException e) {
			log.error(">>>as_flink<<<资源文件加载失败!");
			e.printStackTrace();
		}
		log.info(">>>as_flink<<<资源文件加载成功");
	}

	public static String getStrValue(String key) {
		return contextProperties.getProperty(key);
	}

	public static int getIntValue(String key) {
		String strValue = getStrValue(key);
		return Integer.parseInt(strValue);
	}

	public static Properties getKafkaProperties(String groupId) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", getStrValue("kafka.bootstrap.servers"));
		properties.setProperty("zookeeper.connect", getStrValue("kafka.zookeeper.connect"));
		properties.put("enable.auto.commit", getStrValue("enable.auto.commit"));
		properties.put("auto.commit.interval.ms", getStrValue("auto.commit.interval.ms"));
		properties.put("auto.offset.reset", getStrValue("auto.offset.reset"));
		properties.put("session.timeout.ms", getStrValue("session.timeout.ms"));
		properties.setProperty("group.id", groupId);
		return properties;
	}

}