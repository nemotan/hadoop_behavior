package com.landray.behavior.request.input;

import java.util.regex.Pattern;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class JavaParser extends Parser {
	private String[] ignores = { "/resource/*", "/sys/ui/*",
			"/sys/mobile/js/*", "/login.jsp*", "/index.jsp", "/sys/index.jsp",
			"*.version", "*.html", "*.htm", "*.xml", "/sys/nav.jsp",
			"/dbcenter/behavior/*", "*/lang.jsp", "*/placeholder.jsp",
			"/admin.do*", "/sys/admin/*" };

	private JavaDefaultParser defaultParser = new JavaDefaultParser();

	@Override
	public UrlContent execute(UrlContent content) throws CannotParserException {
		// 格式化地址
		String request = content.request.replaceAll("/+", "/");
		int index = request.indexOf(";jsessionid=");
		if (index > -1) {
			request = request.substring(0, index);
		}
		content.request = request = request.endsWith("/") ? request
				+ "index.jsp" : request;
		// 排除特殊地址
		if ("/j_acegi_security_check".equals(request)) {
			content.path = request;
			return content;
		}
		for (String ignore : ignores) {
			if (ignore.endsWith("*")) {
				if (request
						.startsWith(ignore.substring(0, ignore.length() - 1))) {
					return null;
				}
			} else if (ignore.startsWith("*")) {
				if (request.endsWith(ignore.substring(1))) {
					return null;
				}
			} else {
				if (request.equals(ignore)) {
					return null;
				}
			}
		}
		if (request.endsWith(".index") && !request.endsWith("/.index")) {
			return null;
		}
		// 检查非法路径
		checkRequest(request);
		// 查找解析器并执行
		Parser parser = defaultParser;
		JSONArray modules = config.getJSONArray("modules");
		for (int i = 0; i < modules.size(); i++) {
			JSONObject module = modules.getJSONObject(i);
			if (request.startsWith(module.getString("path"))) {
				parser = buildParser(module.getString("parser"));
				parser.setConfig(module);
				parser.setDefaultParser(defaultParser);
				break;
			}
		}
		return parser.execute(content);
	}

	private void checkRequest(String request) throws CannotParserException {
		String[] paths = request.split("/");
		int n = paths.length - 1;
		// 前面的路径，不允许出现“A-Za-z0-9_”的之外字符
		Pattern p = Pattern.compile("\\W");
		for (int i = 0; i < n; i++) {
			if (p.matcher(paths[i]).find()) {
				throw new CannotParserException();
			}
		}
		// 最后一部分，允许出现.但不能出现超过一次
		String path = paths[n];
		int index = path.indexOf(".");
		if (index > -1) {
			if (p.matcher(path.substring(index + 1)).find()) {
				throw new CannotParserException();
			}
			path = path.substring(0, index);
		}
		if (p.matcher(path.replace('.', '_').replace('-', '_')).find()) {
			throw new CannotParserException();
		}
	}

	@Override
	public void setConfig(JSONObject config) {
		super.setConfig(config);
		defaultParser.setConfig(config);
	}
}
