package com.bigdata.spider.service.impl;

import java.io.UnsupportedEncodingException;

import org.apache.commons.lang.StringUtils;

import com.bigdata.spider.entity.Page;
import com.bigdata.spider.service.IProcessService;
import com.bigdata.spider.util.PageDownLoadUtil;
import com.bigdata.spider.util.ParseAndDecodeUtil;


/**
 * 页面解析接口实现类
 * @author Administrator
 * created by 20170413
 */
public class IProcessServiceImpl implements IProcessService{
	
	public Page process(Page page)  {
		// 获得需要解析的内容
		Page processPage=page;
		String newsUrl=page.getUrl();
		
		if(StringUtils.isNotBlank(newsUrl)&&newsUrl.length()>40 && newsUrl.contains("doc")){
			try {
				String pageContent = PageDownLoadUtil.getPageContent(newsUrl);
				page.setCatgory(ParseAndDecodeUtil.getCatgory(pageContent));
				page.setTitle(ParseAndDecodeUtil.getTitle(pageContent));
				page.setContent(ParseAndDecodeUtil.getContent(pageContent));
				page.setCommentCount(ParseAndDecodeUtil.getCommentCount(newsUrl));
				page.setKeywords(ParseAndDecodeUtil.getKeywords(pageContent));
				page.setNewsFrom(ParseAndDecodeUtil.getNewsFrom(pageContent));
				page.setTime(ParseAndDecodeUtil.getTime(pageContent));
				return processPage;
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
		
	
	public static void main(String[] args) {
//	    IProcessServiceImpl processUrl=new IProcessServiceImpl();
//	    List<Page> processResult = processUrl.process(download);
	}
}
