package com.bigdata.spider.service.impl;

import org.apache.commons.lang.StringUtils;

import com.bigdata.spider.service.IRepositoryService;
import com.bigdata.spider.util.RedisUtil;

/**
 * Redis url仓库实现类
 * 
 * @author ibf
 * 
 */
public class RedisRepositoryServiceImpl implements IRepositoryService {
	RedisUtil redisUtil = new RedisUtil();

	public String poll() {
		String url = redisUtil.poll(RedisUtil.websiteurl);
		if (StringUtils.isBlank(url)) {
			System.exit(0);
		}
		return url;
	}
	
	public void addWebsiteurl(String url) {
		redisUtil.addSet(RedisUtil.websiteurl, url);;
	}

	public String getWebsiteurl() {
		return redisUtil.getSet(RedisUtil.websiteurl);
	}

}
