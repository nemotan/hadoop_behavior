package com.landray.behavior.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.landray.behavior.job.hotspot.HotSpotPointJob;

public class JobManager {
	protected static final Log LOG = LogFactory.getLog(JobManager.class);

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			args =  new String[1];
			args[0] = "2015-08-07";
			//LOG.error("参数不能为空!该参数为date:YYYY-MM-DD格式");
			//return;
		}
		//new HotSpotWidgetJob().run(args[0]);
		new HotSpotPointJob().run(args[0]);
	}
}
