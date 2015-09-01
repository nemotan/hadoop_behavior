package com.landray.behavior.util;

import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	public static final long SECOND = 1000;

	public static final long MINUTE = SECOND * 60;

	public static final long HOUR = MINUTE * 60;

	public static final long DAY = HOUR * 24;

	public static final long WEEK = DAY * 7;

	public static final long MONTH = DAY * 30;

	public static final long YEAR = DAY * 365;

	public static String toString(Date date, String pattern) {
		SimpleDateFormat df = new SimpleDateFormat(pattern);
		return df.format(date);
	}
	
   /**
    * 把date字符串类型转换成long类型
    * 
    * @param dateStr YYYY-MM-DD 格式
    * @return
 * @throws ParseException 
    * @throws Exception
    */
	public static long convertDateStringToLong(String dateStr) throws Exception {
         Format f = new SimpleDateFormat("yyyy-MM-dd");
         Date d = (Date) f.parseObject(dateStr);
         return d.getTime();
	}
}
