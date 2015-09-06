package com.landray.behavior.request.input;

public class EmptyParser extends Parser {
	@Override
	public UrlContent execute(UrlContent content) throws CannotParserException {
		content.path = content.request;
		return content;
	}
}
