package com.bigdata.spider.service;


/**
 * 存储url仓库接口
 * @author ibf
 *
 */
public interface IRepositoryService {
	
	//这个是从队列拉拉取的
	public String poll();
	
	public void addWebsiteurl(String url);
	
	public String getWebsiteurl();
}
