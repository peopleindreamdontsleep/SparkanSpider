package com.bigdata.spider.service;


import com.bigdata.spider.entity.Page;

/**
 * 页面下载接口
 * @author dongxie
 * created by 20170413
 */

public interface IDownLoadService {
	
	//根据RedisRepositoryServiceImpl里面poll取出来的url赋值给page对象
	public Page download(String url);

}
