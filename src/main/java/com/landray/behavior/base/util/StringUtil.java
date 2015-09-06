package com.landray.behavior.base.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {
	public static String getParameter(String query, String param) {
		if (query == null) {
			return null;
		}
		Pattern p = Pattern.compile("&" + param + "=([^&]*)");
		Matcher m = p.matcher("&" + query);
		String value = null;
		if (m.find()) {
			value = m.group(1);
		}
		if (value != null) {
			try {
				value = URLDecoder.decode(value, "UTF-8");
			} catch (UnsupportedEncodingException e) {
			}
		}
		return value;
	}
}
