package com.bigdata.spider.service;


import com.bigdata.spider.entity.Page;

/**
 * 页面解析接口
 * @author dongxie
 * created by 20170413
 */
public interface IProcessService {
	
	//这里是具体对页面进行解析，拿到我们需要的新闻内容
	public Page process(Page pages);

}
