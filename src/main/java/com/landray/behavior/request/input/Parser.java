package com.landray.behavior.request.input;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class Parser {
	protected static final Log logger = LogFactory.getLog(Parser.class);

	protected JSONObject config;

	protected Parser defaultParser;

	public void setConfig(JSONObject config) {
		this.config = config;
	}

	public void setDefaultParser(Parser defaultParser) {
		this.defaultParser = defaultParser;
	}

	public abstract UrlContent execute(UrlContent content)
			throws CannotParserException;

	protected Parser buildParser(String className) throws CannotParserException {
		if (className.indexOf(".") == -1) {
			className = getClass().getPackage().getName() + "." + className;
		}
		try {
			return (Parser) Class.forName(className).newInstance();
		} catch (Exception e) {
			logger.error("实例化解释器错误", e);
			throw new CannotParserException();
		}
	}

	protected boolean equals(String value0, String... values) {
		for (String value : values) {
			if (value.equals(value0)) {
				return true;
			}
		}
		return false;
	}
}
