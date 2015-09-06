package com.landray.behavior.base.name;

public class Model extends Name {
	private static final long serialVersionUID = 6363950437050655041L;

	/** 将kmReviewMain转换成KmReviewMain */
	public static String toModelName(String name) {
		char c = name.charAt(0);
		if (Character.isLowerCase(c)) {
			return Character.toUpperCase(c) + name.substring(1);
		}
		return name;
	}
}
