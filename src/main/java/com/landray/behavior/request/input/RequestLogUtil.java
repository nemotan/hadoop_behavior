package com.landray.behavior.request.input;

import com.landray.behavior.base.name.Custom;
import com.landray.behavior.base.name.CustomManager;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by nemo on 15-9-6.
 */
public class RequestLogUtil {
    private static final Log logger = LogFactory
            .getLog(RequestLogUtil.class);
    /**
     * 把日志解析成JSON格式
     *
     * @param line
     * @return
     */
    public static JSONObject getLogJSON(String line) {
        //url错误列表
        List<String> errorList = new ArrayList<String>();

        JSONObject lineJson = JSONObject.fromObject(line);
        String id = lineJson.get("id").toString();// 客户id
        String flleName = lineJson.getString("fileName").toString();// 文件名
        String log = lineJson.getString("value");// 日志内容

        //根据ID获取custom信息
        Custom custom = CustomManager.getCustom(id);

        String nodeName = getNodeName(flleName);
        String[] info = log.split("\t");

        JSONObject doc = new JSONObject();
        doc.put("create",System.currentTimeMillis());
        long dt = Long.valueOf(info[12]);
        doc.put("node", nodeName);
        doc.put("session", info[1]);
        doc.put("time", Long.valueOf(info[2]) - dt);
        doc.put("ip", info[3]);
        doc.put("user", info[4]);
        doc.put("name", decodeUser(info[5]));
        doc.put("ua", Integer.valueOf(info[6]));
        doc.put("browser", info[7]);
        doc.put("browserVer", info[8]);
        doc.put("type", formatContentType(info[9]));
        doc.put("dt", dt);

        // URL
        UrlContent url = formatUrl(info[10], custom, errorList);
        if (url == null) {
            return null;
        } else {
            doc.put("url", url.toDBObject());
            if (url.contentType != null) {
                doc.put("type", url.contentType);
            }
        }
        // REF
        url = formatUrl(info[11], custom, errorList);
        if (url == null) {
            doc.put("ref", null);
        } else {
            doc.put("ref", url.toDBObject());
        }

        //打印解析出错的url
        if (logger.isDebugEnabled() && !errorList.isEmpty()) {
            StringBuffer sb = new StringBuffer("无法解释的URL：");
            for (String err : errorList) {
                sb.append("\r\n").append(err);
            }
            logger.debug(sb);
        }

        return doc;
    }


    private static String decodeUser(String user) {
        try {
            return URLDecoder.decode(user, "UTF-8");
        } catch (Exception e) {
            return user;
        }
    }

    private static String formatContentType(String contentType) {
        if (StringUtils.isBlank(contentType)) {
            return "";
        }
        return contentType;
    }

    private static String getNodeName(String fileName) {
        String[] fileInfo = fileName.split("\\.");
        if (fileInfo.length == 3) {
            return "default";
        }
        StringBuffer result = new StringBuffer();
        for (int i = 1; i < fileInfo.length - 2; i++) {
            result.append('.').append(fileInfo[i]);
        }

        return result.substring(1);
    }
    private static UrlContent formatUrl(String url, Custom custom,
                                 List<String> errorList) {
        if (StringUtils.isBlank(url)) {
            return null;
        }
        try {
            return UrlParser.parse(url, custom);
        } catch (CannotParserException e) {
            errorList.add(url);
        }
        return null;
    }

}