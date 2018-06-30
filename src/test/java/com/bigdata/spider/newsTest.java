package com.bigdata.spider;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.spider.util.PageDownLoadUtil;

public class newsTest {
	
	public static String urlTransForm(String url){
		String urlTransForm = PageDownLoadUtil.getPageContent(url);
		String urlRegex="<meta name=\"comment\" content=.*?>";
    	Pattern pattern=Pattern.compile(urlRegex);
		Matcher matcher1=pattern.matcher(urlTransForm);
		String trUrl="";
		if(matcher1.find()){
			String result=matcher1.group(0);
			String channel=result.split("\"")[3].split(":")[0];
			String cmos=result.split("\"")[3].split(":")[1];
			trUrl="http://comment5.news.sina.com.cn/page/info?version=1&format=js&channel="+channel+"&newsid="+cmos+"&group=&compress=0&ie=utf-8&oe=utf-8";
		}
		return trUrl;
	}
	
	public static String getJson(String url){
		
		String pageContent = PageDownLoadUtil.getPageContent(url);
		int substing=pageContent.split("=")[0].length()+1;
		String transting=pageContent.substring(substing, pageContent.length());
		return transting;
	}
	
	public static String getCommentFJson(String trurl){
		
		String json = getJson(trurl);
		JSONObject parseObject = JSON.parseObject(json);
		Object object = parseObject.get("result");
		JSONObject parseObject1 = JSON.parseObject(object.toString());
		Object object1 = parseObject1.get("count");
		
		JSONObject parseObject2 = JSON.parseObject(object1.toString());
		Object object2 = parseObject2.get("total");
		System.out.println("评论数："+object2);
		return object2.toString();
	}
	
	
	public static void main(String[] args) {
		String urlTransForm = urlTransForm("http://mil.news.sina.com.cn/jssd/2017-06-28/doc-ifyhmtcf2990745.shtml");
		
	}

}
