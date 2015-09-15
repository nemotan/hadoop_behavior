package com.landray.behavior.base.db;

import com.landray.behavior.base.util.Config;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Date;
import java.util.List;

public class MongoPool {
	private static final Log logger = LogFactory.getLog(MongoPool.class);

	private static MongoPool instance = new MongoPool();

	private MongoClient client;

	public static MongoPool getInstance() {
		return instance;
	}

	private MongoPool() {
		try {

			String host = Config.getProperty("mongo.host");
			String port = Config.getProperty("mongo.port");
			if (port == null) {
				client = new MongoClient(host);
			} else {
				client = new MongoClient(host, Integer.valueOf(port));
			}
		} catch (Exception e) {
			logger.error("加载配置文件错误", e);
		}
	}

	public DB getDB(String dbname) {
		return client.getDB(dbname);
	}

	public List<String> getDBNames() {
		return client.getDatabaseNames();
	}

	public Date getStartTime() {
		DBCursor cursor = MongoPool.getInstance().getDB("local").getCollection(
				"startup_log").find();
		try {
			cursor.sort(new BasicDBObject("startTime", -1)).limit(1);
			return (Date) cursor.next().get("startTime");
		} finally {
			cursor.close();
		}
	}
}
