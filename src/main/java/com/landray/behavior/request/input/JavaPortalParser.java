package com.landray.behavior.request.input;

import com.landray.behavior.base.util.StringUtil;

public class JavaPortalParser extends Parser {

	@Override
	public UrlContent execute(UrlContent content) throws CannotParserException {
		content = defaultParser.execute(content);
		if (content == null) {
			return null;
		}
		if (content.query == null) {
			return content;
		}
		content.extendProperty.put("fdId", StringUtil.getParameter(
				content.query, "pageId"));
		return content;
	}

}
