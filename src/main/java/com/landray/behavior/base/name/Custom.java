package com.landray.behavior.base.name;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.landray.behavior.base.db.DBNames;
import com.landray.behavior.base.db.MongoPool;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class Custom {
	private String id;

	public Custom(String id) {
		super();
		this.id = id;
	}

	/** 客户ID */
	public String getId() {
		return id;
	}

	/** 读取数据 */
	private <T extends Name> List<T> loadList(String collName, Class<T> clazz,
			boolean custom) {
		// 从数据库加载
		List<T> list = new ArrayList<T>();
		DBCollection coll = MongoPool.getInstance().getDB(DBNames.BASE_DB)
				.getCollection(collName);
		DBCursor cursor;
		if (custom) {
			cursor = coll.find(new BasicDBObject("custom", id));
		} else {
			cursor = coll.find();
		}
		try {
			while (cursor.hasNext()) {
				list.add(docToName(clazz, cursor.next()));
			}
		} finally {
			cursor.close();
		}
		// 排序
		Collections.sort(list);
		return list;
	}

	/** 数据库对象转成Name对象 */
	@SuppressWarnings("unchecked")
	private <T extends Name> T docToName(Class<T> clazz, DBObject doc) {
		T name;
		try {
			name = clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		name.setKey((String) doc.get("_id"));
		name.installValues(doc.toMap());
		return name;
	}

	/** 保存数据库对象和缓存对象 */
	private <T extends Name> List<T> saveList(String collName, List<T> list,
			Class<T> clazz, boolean replace, boolean custom) {
		DBCollection coll = MongoPool.getInstance().getDB(DBNames.BASE_DB)
				.getCollection(collName);
		// 加载原有数据
		List<T> oldList = loadList(collName, clazz, custom);
		List<T> newList = new ArrayList<T>(list);
		if (!replace) {
			newList.removeAll(oldList);
		}
		for (Name name : newList) {
			BasicDBObject doc = nameToDoc(name);
			if (custom) {
				doc.put("custom", id);
			}
			coll.save(doc);
		}
		// 将原有数据合并进来
		if (oldList != null) {
			for (T name : oldList) {
				if (!newList.contains(name)) {
					newList.add(name);
				}
			}
		}
		// 重新排序，写入缓存
		Collections.sort(newList);
		return newList;
	}

	/** Name对象转成数据库对象 */
	private BasicDBObject nameToDoc(Name name) {
		BasicDBObject doc = new BasicDBObject("_id", name.getKey());
		doc.putAll(name.valueMap());
		return doc;
	}

	/** 匹配是否以Key开始 */
	private <T extends Name> T matchStart(List<T> names, String s) {
		for (T name : names) {
			if (s.startsWith(name.getKey())) {
				return name;
			}
		}
		return null;
	}

	/** 匹配是否等于 */
	private <T extends Name> T matchAll(List<T> names, String s) {
		for (T name : names) {
			if (s.equals(name.getKey())) {
				return name;
			}
		}
		return null;
	}

	private List<Server> servers;

	/** 获取所有服务器列表 */
	public List<Server> getServers() {
		if (servers == null) {
			servers = loadList("servers", Server.class, true);
		}
		return servers;
	}

	/** 保存服务器列表 */
	public void saveServers(List<Server> servers) {
		DBCollection coll = MongoPool.getInstance().getDB(DBNames.BASE_DB)
				.getCollection("servers");
		coll.findAndRemove(new BasicDBObject("custom", id));
		this.servers = saveList("servers", servers, Server.class, true, true);
	}

	public Server getDefaultServer() {
		for (Server server : getServers()) {
			if (server.isDefaultServer()) {
				return server;
			}
		}
		return null;
	}

	/** 根据URL查找服务器信息 */
	public Server findServer(String path) {
		if (path.startsWith("/")) {
			return getDefaultServer();
		}
		return matchStart(getServers(), path.toLowerCase());
	}

	private List<Module> modules;

	/** 获取模块列表 */
	public List<Module> getModules() {
		if (modules == null) {
			modules = loadList("modules", Module.class, false);
		}
		return modules;
	}

	/** 保存模块列表 */
	public void saveModules(List<Module> modules, boolean replace) {
		this.modules = saveList("modules", modules, Module.class, replace,
				false);
	}

	/** 根据模块路径精确查找Module */
	public Module getModule(String modulePath) {
		return matchAll(getModules(), modulePath);
	}

	/** 模糊查找Module */
	public Module findModule(String path) {
		return matchStart(getModules(), Module.toModulePath(path));
	}

	/** 智能判断模块路径，即便模块未注册 */
	public String findUnsafeModule(String path) {
		// 先到已有的模块找一下
		int index = path.indexOf("/");
		String name;
		if (index > -1) {
			name = path;
		} else {
			name = Module.toModulePath(path);
		}
		Module module = matchStart(getModules(), name + "/");
		if (module != null) {
			return module.getKey();
		}
		// 找不到，若path是路径，则取前两个目录
		if (index == 0) {
			String[] paths = path.split("/");
			if (paths.length > 3) {
				return "/" + paths[1] + "/" + paths[2] + "/";
			}
		}
		return null;
	}

	private List<Model> models;

	/** 获取Model列表 */
	public List<Model> getModels() {
		if (models == null) {
			models = loadList("models", Model.class, false);
		}
		return models;
	}

	/** 保存列表 */
	public void saveModels(List<Model> models, boolean replace) {
		this.models = saveList("models", models, Model.class, replace, false);
	}

	/** 精确查找Model，不管首字母大小写 */
	public Model getModel(String name) {
		return matchAll(getModels(), Model.toModelName(name));
	}

	/** 模糊查找Model，不管首字母大小写 */
	public Model findModel(String name) {
		return matchStart(getModels(), Model.toModelName(name));
	}

	private List<Method> methods;

	/** 获取Method列表 */
	public List<Method> getMethods() {
		if (methods == null) {
			methods = loadList("methods", Method.class, false);
		}
		return methods;
	}

	/** 保存列表 */
	public void saveMethods(List<Method> methods, boolean replace) {
		this.methods = saveList("methods", methods, Method.class, replace,
				false);
	}

	/** 精确查找Method */
	public Method getMethod(String name) {
		return matchAll(getMethods(), name);
	}

	private Map<String, Path> pathMap = new HashMap<String, Path>();

	/** 获取路径 */
	public Path getPath(String name) {
		// 从内存中取
		Path path = pathMap.get(name);
		if (path == null) {
			// 从数据库取
			DBObject doc = MongoPool.getInstance().getDB(DBNames.BASE_DB)
					.getCollection("paths").findOne(name);
			if (doc == null) {
				path = new Path();
			} else {
				path = docToName(Path.class, doc);
			}
			pathMap.put(name, path);
		}
		return path.getKey() == null ? null : path;
	}

	/** 保存路径 */
	public void savePath(Path path) {
		if (path.getKey() == null) {
			return;
		}
		DBCollection coll = MongoPool.getInstance().getDB(DBNames.BASE_DB)
				.getCollection("paths");
		coll.save(nameToDoc(path));
		pathMap.put(path.getKey(), path);
	}
}
