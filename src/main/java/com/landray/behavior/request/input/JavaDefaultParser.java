package com.landray.behavior.request.input;

import com.landray.behavior.base.util.StringUtil;

public class JavaDefaultParser extends Parser {
	@Override
	public UrlContent execute(UrlContent content) throws CannotParserException {
		String request = content.request;
		// 解释模块
		String module = content.custom.findUnsafeModule(request);
		if (module == null) {
			throw new CannotParserException();
		}
		content.module = module;

		if (request.length() == content.module.length()
				|| request.length() == content.module.length() - 1) {
			content.path = "index.jsp";
		} else {
			content.path = request.substring(content.module.length());
		}
		if (content.path.endsWith(".do") && content.query != null) {
			content.method = StringUtil.getParameter(content.query, "method");
		}
		return content;
	}
}
