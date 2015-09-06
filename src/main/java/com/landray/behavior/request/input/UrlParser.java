package com.landray.behavior.request.input;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.json.JSONObject;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.landray.behavior.base.name.Custom;
import com.landray.behavior.base.name.Server;

public class UrlParser {
	private static final Log logger = LogFactory.getLog(UrlParser.class);

	private static UrlParser instance = new UrlParser();

	public static UrlContent parse(String url, Custom custom)
			throws CannotParserException {
		return instance.doParse(url, custom);
	}

	private JSONObject config;

	private String[] ignores = { ".js", ".swf", ".png", ".jpg", ".jpeg",
			".gif", ".bmp", ".ico", ".css", ".tmpl", ".ttf", ".cur" };

	private Map<String, Parser> parserMap = new ConcurrentHashMap<String, Parser>();

	private UrlParser() {
		try {
			loadConfig();
		} catch (Exception e) {
			logger.error("加载配置信息错误", e);
		}
	}

	private void loadConfig() throws Exception {
		InputStream in = UrlParser.class.getResourceAsStream("parser.js");
		try {
			String s = IOUtils.toString(in, "UTF-8").trim();
			if (s.startsWith("return")) {
				s = s.substring(6);
			}
			if (s.endsWith(";")) {
				s = s.substring(0, s.length() - 1);
			}
			config = JSONObject.fromObject(s);
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}

	private UrlContent doParse(String url, Custom custom)
			throws CannotParserException {
		// 查找解释器
		Server server = custom.findServer(url);
		Parser parser;
		String serverName;
		if (server == null) {
			serverName = parseServer(url);
			parser = new EmptyParser();
		} else {
			serverName = server.getKey();
			parser = findParser(server.getType());
		}
		String path = url;
		if (!url.startsWith("/")) {
			path = url.substring(serverName.length());
		}

		// 解释URL，到这里，path不包含服务器路径
		UrlContent content = new UrlContent();
		content.custom = custom;
		content.server = serverName;

		// 拆分请求地址（request）、参数（query）和锚（#hash）
		int index = path.indexOf("?");
		if (index > -1) {
			content.request = path.substring(0, index).trim();
			path = path.substring(index + 1);
			index = path.indexOf("#");
			if (index > -1) {
				content.query = path.substring(0, index).trim();
				content.hash = path.substring(index + 1).trim();
			} else {
				content.query = path.trim();
			}
		} else {
			index = path.indexOf("#");
			if (index > -1) {
				content.request = path.substring(0, index).trim();
				content.hash = path.substring(index + 1).trim();
			} else {
				content.request = path.trim();
			}
		}
		String request = content.request;
		if (request.equals("")) {
			// 根地址统一
			content.request = "/";
		} else if (!request.startsWith("/")) {
			// 相对路径解释不了，不处理
			return null;
		}

		// 忽略地址
		for (String ignore : ignores) {
			if (request.endsWith(ignore)) {
				return null;
			}
		}
		return parser.execute(content);
	}

	private Parser findParser(String key) throws CannotParserException {
		Parser parser = parserMap.get(key);
		if (parser == null) {
			JSONObject parserCfg = (JSONObject) config.get(key);
			if (parserCfg == null) {
				parser = new EmptyParser();
			} else {
				String className = parserCfg.getString("parser");
				if (className.indexOf(".") == -1) {
					className = UrlParser.class.getPackage().getName() + "."
							+ className;
				}
				try {
					parser = (Parser) Class.forName(className).newInstance();
				} catch (Exception e) {
					logger.error("实例化解释器错误", e);
					throw new CannotParserException();
				}
			}
			parser.setConfig(parserCfg);
			parserMap.put(key, parser);
		}
		return parser;
	}

	private String parseServer(String url) throws CannotParserException {
		int index = url.indexOf("://");
		if (index == -1) {
			throw new CannotParserException();
		}
		int index2 = url.indexOf("/", index + 3);
		if (index2 == -1) {
			return url;
		} else {
			return url.substring(0, index2);
		}
	}
}
