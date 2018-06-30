package com.bigdata.spider.service;

import com.bigdata.spider.entity.Page;

/**
 * 存储接口
 * @author dongxie
 * created by 20170413
 */
public interface IStoreService {
	
	//这里是存储Hbase的接口，存储时我们解析出来的page对象
	public void store(Page page);

}
