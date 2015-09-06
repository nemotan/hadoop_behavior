package com.landray.behavior.request.input;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.landray.behavior.base.name.Custom;
import com.landray.behavior.base.util.StringUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class UrlContent {
	public Custom custom;
	public String request;

	public String server;
	public String module;
	public String path;
	public String method;
	public String query;
	public String hash;
	public String contentType;

	public Map<String, Object> extendProperty = new HashMap<String, Object>();

	public DBObject toDBObject() {
		BasicDBObject doc = new BasicDBObject();
		doc.put("server", server);
		doc.put("module", module);
		doc.put("path", path);
		doc.put("method", method);
		doc.put("query", query);
		doc.put("hash", hash);
		doc.put("portlet", StringUtil.getParameter(query, "LUIID") != null);
		for (Entry<String, Object> entry : extendProperty.entrySet()) {
			doc.put(entry.getKey(), entry.getValue());
		}
		return doc;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		if (server != null) {
			sb.append(server);
		}
		if (module != null) {
			sb.append(module);
		}
		if (path != null) {
			sb.append(path);
		}
		if (method != null) {
			sb.append("?method=").append(method);
		}
		return sb.toString();
	}
}
