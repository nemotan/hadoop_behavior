package com.landray.behavior.request.input;

import com.landray.behavior.base.util.StringUtil;

public class JavaIndexParser extends Parser {
	@Override
	public UrlContent execute(UrlContent content) throws CannotParserException {
		if (content.query == null) {
			return null;
		}
		String nav = StringUtil.getParameter(content.query, "nav");
		if (nav == null || !nav.startsWith("/")) {
			return null;
		}
		if (nav.endsWith(".jsp")) {
			nav = nav.substring(0, nav.length() - 4);
		}
		UrlContent result = UrlParser.parse(nav, content.custom);
		if (result == null) {
			return null;
		}
		content.module = result.module;
		content.path = ".index";
		content.query = null;
		return content;
	}
}
