package com.bigdata.spider.util;


import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


/**
 * 页面下载工具类
 * @author dongxie
 * created by 20170413
 *
 */

public class PageDownLoadUtil {
	
	//通过HTTPCLIENT获取整个页面
	public static String getPageContent(String url){
		//在方法里创建一个client
		HttpClientBuilder builder=HttpClients.custom();
		
		CloseableHttpClient client=builder.build();
		
		HttpGet request=new HttpGet(url);
		
		String content=null;
		
		try {
			
			request.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36");
			
			CloseableHttpResponse response=client.execute(request);
			
			//提供了接收方
			HttpEntity entity=response.getEntity();
			
			content=EntityUtils.toString(entity);
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return content;
	}
	
	/**
	 * 测试页面下载工具
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(getPageContent("http://mil.news.sina.com.cn/"));
		
	}
	
	
}
