package com.bigdata.spider;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.spider.util.PageDownLoadUtil;

import junit.framework.TestCase;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase{
	
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
		return trUrl.toString();
	}
	
	public static String getJson(String url){
		
		String pageContent = PageDownLoadUtil.getPageContent(url);
		int substing=pageContent.split("=")[0].length()+1;
		String transting=pageContent.substring(substing, pageContent.length());
		return transting;
	}
	
	public static void main(String[] args) {
		
		String jsonTest="{\"D_cardNo\":\"4444444444444444\",\"D_debitCreditFlg\":\"2\",\"D_isSmsReply\":\"Y\",\"D_isCSRsn\":\"Y\",\"D_channelFlag\":\"0\",\"D_rollOutMemo\":\"随便备注一下\"}";
		
		JSONObject parseObject = JSON.parseObject(jsonTest);
		
		String aa = parseObject.getString("D_cardNo");
		System.out.println(aa);
	
	
	}
}
