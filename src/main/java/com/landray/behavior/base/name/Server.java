package com.landray.behavior.base.name;

import java.util.Map;

public class Server extends Name {
	private static final long serialVersionUID = 7521038868254958301L;

	protected String type;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	protected boolean defaultServer;

	public boolean isDefaultServer() {
		return defaultServer;
	}

	public void setDefaultServer(boolean defaultServer) {
		this.defaultServer = defaultServer;
	}

	@Override
	public boolean check() {
		key = key.toLowerCase();
		return super.check();
	}

	@Override
	public Map<String, Object> valueMap() {
		Map<String, Object> map = super.valueMap();
		map.put("type", type);
		map.put("defaultServer", defaultServer);
		return map;
	}

	@Override
	public void installValues(Map<String, Object> map) {
		super.installValues(map);
		type = (String) map.get("type");
		Object value = map.get("defaultServer");
		if (value != null) {
			defaultServer = (Boolean) value;
		}
	}
}
