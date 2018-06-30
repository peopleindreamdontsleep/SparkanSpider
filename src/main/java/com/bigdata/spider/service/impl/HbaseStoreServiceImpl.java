package com.bigdata.spider.service.impl;

import com.bigdata.spider.entity.Page;
import com.bigdata.spider.service.IStoreService;
import com.bigdata.spider.util.HbaseUtil;

public class HbaseStoreServiceImpl implements IStoreService{
	/**
	 * create 'sina',{NAME =>'news',VERSION =>30}
	 * 具体将page中的对象存储到hbase里面
	 */
	HbaseUtil hbaseutil=new HbaseUtil();

	public void store(Page page) {
		
		String newsUrl=page.getUrl();
		
		hbaseutil.put("sina", newsUrl, "news", "catgory", page.getCatgory());
		hbaseutil.put("sina", newsUrl, "news", "title", page.getTitle());
		hbaseutil.put("sina", newsUrl, "news", "url", page.getUrl());
		hbaseutil.put("sina", newsUrl, "news", "time", page.getTime());
		hbaseutil.put("sina", newsUrl, "news", "newsFrom", page.getNewsFrom());
		hbaseutil.put("sina", newsUrl, "news", "commentCount", page.getCommentCount());
		hbaseutil.put("sina", newsUrl, "news", "content", page.getContent());
		hbaseutil.put("sina", newsUrl, "news", "keywords", page.getKeywords());
		
		System.out.println("本次存到Hbaseok");

	}
	
	
}
