package com.landray.behavior.base.name;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class Name implements Serializable, Comparable<Name> {
	private static final long serialVersionUID = -7014381456801842785L;

	protected String key;

	protected String name;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Map<String, Object> valueMap() {
		Map<String, Object> values = new HashMap<String, Object>();
		values.put("name", getName());
		return values;
	}

	public void installValues(Map<String, Object> map) {
		setName((String) map.get("name"));
	}

	public boolean check() {
		return StringUtils.isNotBlank(name);
	}

	@Override
	public int compareTo(Name other) {
		return -getKey().compareTo(other.getKey());
	}

	@Override
	public int hashCode() {
		HashCodeBuilder rtnVal = new HashCodeBuilder(-426830461, 631494429);
		rtnVal.append(getClass());
		rtnVal.append(getKey());
		return rtnVal.toHashCode();
	}

	public boolean equals(Object object) {
		if (this == object)
			return true;
		if (object == null)
			return false;
		if (!getClass().equals(object.getClass()))
			return false;
		Name other = (Name) object;
		return getKey().equals(other.getKey());
	}
}
