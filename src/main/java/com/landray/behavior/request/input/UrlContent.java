package com.landray.behavior.request.input;

import com.landray.behavior.base.name.Custom;
import com.landray.behavior.base.util.StringUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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

	public String toHiveMapString(){
		StringBuffer sb = new StringBuffer();
		String CONN = ",";
		sb.append("server:").append(server).append(CONN);
		sb.append("module:").append(module).append(CONN);
		sb.append("path:").append(path).append(CONN);
		sb.append("method:").append(method).append(CONN);
		sb.append("query:").append(query).append(CONN);
		sb.append("hash:").append(hash).append(CONN);
		sb.append("portlet:").append(StringUtil.getParameter(query, "LUIID") != null).append(CONN);
		for (Entry<String, Object> entry : extendProperty.entrySet()) {
			sb.append(entry.getKey()+":").append(entry.getValue()).append(CONN);
		}
		return sb.toString().substring(0,sb.toString().length()-1);
	}
}


