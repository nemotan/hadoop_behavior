package com.landray.behavior.base.name;

public abstract class CustomManager {
	private static final ThreadLocal<String> CUSTOM_ID = new ThreadLocal<String>();

	private static final ThreadLocal<Custom> CUSTOM = new ThreadLocal<Custom>();

	public static void setCustomId(String customId) {
		if (customId == null) {
			CUSTOM_ID.remove();
			CUSTOM.remove();
		} else {
			CUSTOM_ID.set(customId);
			CUSTOM.set(_getCustom(customId));
		}
	}

	public static String getCustomId() {
		return CUSTOM_ID.get();
	}

	public static Custom getCustom(String customId) {
		if (customId.equals(getCustomId())) {
			return CUSTOM.get();
		}
		return _getCustom(customId);
	}

	private static Custom _getCustom(String customId) {
		return new Custom(customId);
	}
}
