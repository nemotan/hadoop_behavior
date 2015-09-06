package com.landray.behavior.base.util;

import java.util.Iterator;

import com.landray.behavior.base.db.MongoPool;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import net.sf.json.JSONObject;

/**
 *mongodb 工具类
 * 
 * @author nemo
 *
 */
public class DBUtil {
    public static final String JOB_RES = "_res";// job结果数据库
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

    /**
     *
     * @param dbName 数据库名： hotspot_res
     * @param collectionName 集合名：widgets
     * @return
     */
    public static DBCollection getDBCollection(String dbName,String collectionName) {
        DBCollection collection = MongoPool.getInstance().getDB(dbName)
                .getCollection(collectionName);
        return collection;
    }
}
