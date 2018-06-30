package com.bigdata.spider.Demo;

public class Task {

	//图片地址
	public String imageUrl="";
	
	//图片是否被下载了？
	public  boolean hasDownloaded=false;
	
	//图片的名字
	public String filename;
	
	//构造函数，提供图片的URL就可以了
	public Task(String url){
	
		imageUrl=url;
		
		filename=MD5.string2MD5(url);  //对图片加密，利于爬取的各种操作
		
		int last=imageUrl.lastIndexOf(".");
		String ext=imageUrl.substring(last+1);
		filename=filename +"."+ext;
		
		System.out.println("文件名："+filename);
	}
	
	public static void main(String[] args) {
		String url="http://n.sinaimg.cn/mil/crawl/20170623/B424-fyhneam0394260.jpg";
		new Task(url);
	}
	
}
