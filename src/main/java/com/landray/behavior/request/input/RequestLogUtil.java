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

    /*
       hive分隔符
     */
    private static final String TERMINATED = "\t";//hive 分隔符
    /**i
     * 把日志解析成字符串，意导入到hive中
     *
     * @param line
     * @return
     */
    public static String getLogStrHIVE(String line) {
        StringBuffer hiveLine = new StringBuffer();

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

        String type = formatContentType(info[9]);
        //对应hive中map类型 for: job:80,team:60,person:70
        String urlMapStr = "";
        String refMapStr = "";
        // URL
        UrlContent urlContent = formatUrl(info[10], custom, errorList);
        if (urlContent == null) {
            return null;
        } else {
            /*doc.put("url", url.toDBObject());
            if (url.contentType != null) {
                doc.put("type", url.contentType);
            }*/
            type = urlContent.contentType;
            urlMapStr = urlContent.toHiveMapString();
        }
        // REF
        urlContent = formatUrl(info[11], custom, errorList);
        if (urlContent != null) {
            refMapStr = urlContent.toHiveMapString();
        }

        //打印解析出错的url
        if (logger.isDebugEnabled() && !errorList.isEmpty()) {
            StringBuffer sb = new StringBuffer("无法解释的URL：");
            for (String err : errorList) {
                sb.append("\r\n").append(err);
            }
            logger.debug(sb);
        }

        long dt = Long.valueOf(info[12]);

        String fileDate = flleName.split("\\.")[2];
        hiveLine.append(fileDate).append(TERMINATED);//日志fileDate string
        hiveLine.append(id).append(TERMINATED);//日志customerId string

        hiveLine.append(System.currentTimeMillis()).append(TERMINATED);//create BIGINT
        hiveLine.append(nodeName).append(TERMINATED);//node string
        hiveLine.append(info[1]).append(TERMINATED);//session string
        hiveLine.append(Long.valueOf(info[2]) - dt).append(TERMINATED);//time BIGINT
        hiveLine.append(info[3]).append(TERMINATED);//ip string
        hiveLine.append(info[4]).append(TERMINATED);//user string
        hiveLine.append(decodeUser(info[5])).append(TERMINATED);//name string
        hiveLine.append(Integer.valueOf(info[6])).append(TERMINATED);//ua int
        hiveLine.append(info[7]).append(TERMINATED);//browser string
        hiveLine.append(info[8]).append(TERMINATED);//browserVER string
        hiveLine.append(formatContentType(info[9])).append(TERMINATED);//type string
        hiveLine.append(dt).append(TERMINATED);//dt BIGINT
        hiveLine.append(urlMapStr).append(TERMINATED);//url map
        hiveLine.append(refMapStr).append(TERMINATED);//ref map

        //打印解析出错的url
        if (logger.isDebugEnabled() && !errorList.isEmpty()) {
            StringBuffer sb = new StringBuffer("无法解释的URL：");
            for (String err : errorList) {
                sb.append("\r\n").append(err);
            }
            logger.debug(sb);
        }

        return hiveLine.toString();
    }


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
