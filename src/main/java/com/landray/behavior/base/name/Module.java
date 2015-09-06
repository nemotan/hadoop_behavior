package com.landray.behavior.base.name;

public class Module extends Name {
	private static final long serialVersionUID = 3941294653443515007L;

	@Override
	public boolean check() {
		int i1 = key.indexOf('/');
		int i2 = key.lastIndexOf('/');
		if (i1 == -1 || i1 == i2) {
			return false;
		}
		key = key.substring(i1, i2 + 1);
		return super.check();
	}

	/** 将KmReviewMain或kmReviewMain转成成/km/review/main/ */
	public static String toModulePath(String name) {
		if (name.indexOf("/") > -1) {
			return name;
		}
		StringBuffer sb = new StringBuffer();
		boolean upper = false;
		for (int i = 0; i < name.length(); i++) {
			char c = name.charAt(i);
			if (Character.isUpperCase(c)) {
				if (!upper && i > 0) {
					sb.append("/");
				}
				sb.append(Character.toLowerCase(c));
				upper = true;
			} else {
				sb.append(c);
				upper = false;
			}
		}
		return "/" + sb.toString() + "/";
	}
}
