package com.landray.behavior.base.db;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;

public abstract class Config {
	private static final Log logger = LogFactory.getLog(Config.class);

	private static String webContentPath;

	private static Properties properties;

	static {
		reload();
	}

	public synchronized static void reload() {
		InputStream in = null;
		try {
			properties = new Properties();
			in = Thread.currentThread().getContextClassLoader()
					.getResourceAsStream("config.properties");
			properties.load(in);
		} catch (Exception e) {
			logger.error("加载配置文件错误", e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (Exception e) {
				}
			}
		}
	}

	public static String getWebContentPath() {
		if (webContentPath == null) {
			URL url = null;
			try {
				Enumeration<URL> urls = Thread.currentThread()
						.getContextClassLoader().getResources("/");
				while (urls.hasMoreElements()) {
					URL tmpUrl = urls.nextElement();
					String tmpPath = tmpUrl.getPath();
					if (!tmpPath.endsWith("/")) {
						tmpPath += "/";
					}
					if (tmpPath.endsWith("WEB-INF/classes/")) {
						url = tmpUrl;
						break;
					}
				}
				if (null == url) {
					url = Thread.currentThread().getContextClassLoader()
							.getResource("log4j.properties");
				}
			} catch (IOException e) {
			}

			webContentPath = url.getPath().substring(0,
					url.getPath().lastIndexOf("/WEB-INF/"));
			if (webContentPath.startsWith("file:")) {
				webContentPath = webContentPath.substring(5);
			}

			// 打出webContentPath信息,方便查错
			System.out.println("WebContentPath:" + webContentPath + "\n");
		}
		return webContentPath;
	}

	public static String getProperty(String key) {
		return properties.getProperty(key);
	}

	public static String getProperty(String key, String defaultValue) {
		return properties.getProperty(key, defaultValue);
	}

	public static int getProperty(String key, int defaultValue) {
		String value = getProperty(key);
		if (value == null) {
			return defaultValue;
		}
		return Integer.valueOf(value);
	}
}
