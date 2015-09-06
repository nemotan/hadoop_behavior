package com.landray.behavior.request.input;

public class DominoParser extends Parser {

	@Override
	public UrlContent execute(UrlContent content) throws CannotParserException {
		String request = content.request.toLowerCase();
		int index = request.indexOf(".nsf");
		if (index == -1) {
			return null;
		}
		content.module = request.substring(0, index + 4);
		content.path = request.substring(index + 4);
		return content;
	}

}
