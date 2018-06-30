package com.bigdata.spider.Demo;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.zip.GZIPInputStream;


public class HttpUtils {

	//根据url访问服务器，返回服务器响应文本
	public static String doGet(String url) throws Exception 
	{ 
		    //创建一个URL对象，URL
		    URL localURL = new URL(url);
	      
		    //设置代理服务器
		    //System.setProperty("http.proxyHost", "127.0.0.1");  
	       // System.setProperty("http.proxyPort", "8888"); 
		    
	        URLConnection connection = localURL.openConnection();
	        HttpURLConnection httpURLConnection = (HttpURLConnection)connection;
   
	        //设置请求头部的属性
	        httpURLConnection.setRequestProperty("User-Agent","Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E");
	        
	        
	        //保存输入输出流的对象
	        InputStream inputStream = null;
	        InputStreamReader inputStreamReader = null;
	        BufferedReader reader = null;
	        StringBuffer resultBuffer = new StringBuffer();
	        String tempLine = null;
	        
	        //302强制浏览器跳转，200 ok
	        if (httpURLConnection.getResponseCode() >= 300) {
	            throw new Exception("HTTP Request is not success, Response code is " + httpURLConnection.getResponseCode());
	        }
	        
	        try {
	            inputStream = httpURLConnection.getInputStream();
	      
	        
	        	//get header by 'key'
	        	String encoding = httpURLConnection.getHeaderField("Content-Encoding");
	            
	        	//如果返回的是压缩HTML代码
	        	if(encoding!=null && encoding.equals("gzip")){
	        	 System.out.println("这是一个压缩的HTML\n");
	             GZIPInputStream gzin;  
	             gzin = new GZIPInputStream(inputStream); 
	             //对返回页面内容进行utf-8解码，从而中文不会乱码
	             inputStreamReader = new InputStreamReader(gzin,"gbk");
	            
	        	}else{
	        	   inputStreamReader = new InputStreamReader(inputStream,"gbk");
	        	}
	            reader = new BufferedReader(inputStreamReader);
	            
	            while ((tempLine = reader.readLine()) != null) {
	                resultBuffer.append(tempLine+"\n");
	            }
	            
	        } finally {
	            
	            if (reader != null) {
	                reader.close();
	            }
	            
	            if (inputStreamReader != null) {
	                inputStreamReader.close();
	            }
	            
	            if (inputStream != null) {
	                inputStream.close();
	            }
	            
	        }
	        
	        return resultBuffer.toString();
	}
	
	/*
	 * currentBase当前搜索网页的URL
	 * target是从网页标签提取出来的URL(例如href等)
	 * */
	public static String getURL(String currentUrl,String targetUrl)
	{
		String temp=targetUrl;
		//当前页面的路径
		//例如：http://www.gdmec.cn/cs/csnew/index.html
		//应该要分析出:http://www.gdmec.cn/cs/csnew/
		String currentBase="";
		
		String resultURL="";
		
	
		if(currentUrl.endsWith("/"))
		{
			currentBase=currentUrl;
		}
		else
		{
			int lastPos=currentUrl.lastIndexOf("/");
			currentBase=currentUrl.substring(0,lastPos+1);
		}
		
		System.out.println("currentBase:"+currentBase);
		
		if(temp.startsWith("http"))
		{
			return resultURL;
		}
		else if(temp.startsWith("../"))
		{
			//resultURL=currentBase+temp.substring(2);
			
			
		}
		else if(temp.startsWith("./"))
		{
			resultURL=currentBase+temp.substring(2);
		}
		else if(temp.startsWith("//"))
		{
			resultURL="http:"+temp;
		}
		else if(temp.startsWith("/"))
		{
			resultURL=currentBase+temp.substring(1);
		}
		else
		{
			resultURL=currentBase+temp;
		}
		return resultURL;
		
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(doGet("http://mil.news.sina.com.cn/"));
	}
}
