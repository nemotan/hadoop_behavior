package com.landray.behavior.base.name;

import java.io.Serializable;
import java.util.List;

/**
 * 用户属性
 * 
 * @author 谭又豪
 * 
 */
public class User implements Serializable {
	public static final String LOGIN_TYPE_ADMIN = "0";//管理员
	public static final String LOGIN_TYPE_CUSTOM = "1";//普通用户
	public static final String LOGIN_TYPE_IN_ORG = "2";//蓝凌内部用户
 
	String type;// 类型
	String userName;//用户名
	String fdCustomerId;// 客户ID
	String fdCustomerName;// 客户姓名
	List<String[]> scmInfo;// scm项目信息
	boolean isAdmin;//是否为管理员
	


	public User(String userName,String type,boolean isAdmin){
		this.userName = userName;
		this.type = type;
		this.isAdmin = isAdmin;
	}
	
	public String getType() {
		return type;
	}
	
	public void setType(String type) {
		this.type = type;
	}

	
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	
	public String getFdCustomerId() {
		return fdCustomerId;
	}

	public void setFdCustomerId(String fdCustomerId) {
		this.fdCustomerId = fdCustomerId;
	}

	public String getFdCustomerName() {
		return fdCustomerName;
	}

	public void setFdCustomerName(String fdCustomerName) {
		this.fdCustomerName = fdCustomerName;
	}

	public List<String[]> getScmInfo() {
		return scmInfo;
	}

	public void setScmInfo(List<String[]> scmInfo) {
		this.scmInfo = scmInfo;
	}
	

	public boolean isAdmin() {
		return isAdmin;
	}

	public void setAdmin(boolean isAdmin) {
		this.isAdmin = isAdmin;
	}

	public String getOptionString(){
		StringBuffer optionBuffer = new StringBuffer();
		if(this.getScmInfo() == null){
			return "";
		}
		for(String[] item : this.getScmInfo()){
			String key = item[0];
			String value = item[1];
			optionBuffer.append("<option value=\""+key+"\">"+value+"</option>");
		}
		return optionBuffer.toString();
	}

}
