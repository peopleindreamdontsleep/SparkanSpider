package com.bigdata.spider.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class GetCommentUtil {
	
	//获取<meta name=\"comment\" content=.*?>里面直接关系到参数
	public static String urlTransForm(String url){
		String urlTransForm = PageDownLoadUtil.getPageContent(url);
		String urlRegex="<meta name=\"comment\" content=.*?>";
    	Pattern pattern=Pattern.compile(urlRegex);
		Matcher matcher1=pattern.matcher(urlTransForm);
		String trUrl="";
		if(matcher1.find()){
			String result=matcher1.group(0);
			try {
				String channel=result.split("\"")[3].split(":")[0];
				String cmos=result.split("\"")[3].split(":")[1];
				trUrl="http://comment5.news.sina.com.cn/page/info?version=1&format=js&channel="+channel+"&newsid="+cmos+"&group=&compress=0&ie=utf-8&oe=utf-8";
			} catch (Exception e) {
			}
		}
		return trUrl;
	}
	
	public static String getJson(String url){
		if(StringUtils.isNotBlank(url)){
			String pageContent = PageDownLoadUtil.getPageContent(url);
			int substing=pageContent.split("=")[0].length()+1;
			String transting=pageContent.substring(substing, pageContent.length());
			return transting;
		}
		return "";
	}
	
	public static String getCommentFUrl(String url){
		String urlTransForm = urlTransForm(url);
		String json = getJson(urlTransForm);
		if(StringUtils.isNotBlank(json)){
			try {
				JSONObject parseObject = JSON.parseObject(json);
				Object object = parseObject.get("result");
				JSONObject parseObject1 = JSON.parseObject(object.toString());
				Object object1 = parseObject1.get("count");
				
				JSONObject parseObject2 = JSON.parseObject(object1.toString());
				Object object2 = parseObject2.get("total");
				System.out.println("评论数："+object2);
				return object2.toString();
			} catch (Exception e) {
			}
		}
		return "";
	}
	
	public static void main(String[] args) {
		String url="http://mil.news.sina.com.cn/china/2017-07-28/doc-ifyinvyk1803871.shtml";
		urlTransForm(url);
		
	}

}
