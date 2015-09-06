package com.landray.behavior.request.input;

import org.apache.commons.lang.StringUtils;

import com.landray.behavior.base.util.StringUtil;

public class JavaSearchParser extends Parser {

	@Override
	public UrlContent execute(UrlContent content) throws CannotParserException {
		content = defaultParser.execute(content);
		if (content == null) {
			return null;
		}
		if (content.query == null) {
			return content;
		}
		String keyword = StringUtil.getParameter(content.query, "queryString");
		if (StringUtils.isBlank(keyword)) {
			return content;
		}
		keyword = keyword.trim();
		content.extendProperty.put("keyword", keyword);
		String pageno = StringUtil.getParameter(content.query, "pageno");
		pageno = (pageno == null ? "1" : pageno);
		try {
			Integer page = Integer.valueOf(pageno);
			content.extendProperty.put("pageno", page);
		} catch (NumberFormatException e) {
			throw new CannotParserException();
		}
		return content;
	}

}
