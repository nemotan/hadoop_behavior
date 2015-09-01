package com.landray.behavior.util;

import java.util.Iterator;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import net.sf.json.JSONObject;

/**
 *mongodb 工具类
 * 
 * @author nemo
 *
 */
public class DBUtil {
	/**
	 * 把json对象转换成mongodb的DB对象
	 * 
	 * @param jsonObject
	 * @return
	 */
    public static DBObject convertJson2DBObject(JSONObject jsonObject){
    	DBObject valueObject = new BasicDBObject();
        Iterator it = jsonObject.keys();  
        while(it.hasNext()){  
           String key = it.next().toString();
        	valueObject.put(key,jsonObject.get(key));
        }  
        return valueObject;
    }
}
