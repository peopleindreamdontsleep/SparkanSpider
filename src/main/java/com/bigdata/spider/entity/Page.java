package com.bigdata.spider.entity;



/**
 * 页面实体类
 * @author shuai
 * created by
 */
public class Page {
	
	//page的URL
	private String url;
	//属于哪个种类
	private String catgory;
	//标题
	private String title;
	//新闻的日期
	private String time;
	//哪个网站的新闻
	private String newsFrom;
	//累计评价
	private String commentCount;
	//新闻内容
	private String content;
	//新闻关键字
	private String keywords;
	public String getCatgory() {
		return catgory;
	}
	public void setCatgory(String catgory) {
		this.catgory = catgory;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public String getNewsFrom() {
		return newsFrom;
	}
	public void setNewsFrom(String newsFrom) {
		this.newsFrom = newsFrom;
	}
	public String getCommentCount() {
		return commentCount;
	}
	public void setCommentCount(String commentCount) {
		this.commentCount = commentCount;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public String getKeywords() {
		return keywords;
	}
	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}
	
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	
	@Override
	public String toString() {
		return "Page [catgory=" + catgory + ", title=" + title + ", time=" + time + ", newsFrom=" + newsFrom
				+ ", commentCount=" + commentCount + ", content=" + content + ", keywords=" + keywords + "]";
	}
	
	public Page(String catgory, String title, String time, String newsFrom, String commentCount, String content,
			String keywords,String url) {
		super();
		this.catgory = catgory;
		this.title = title;
		this.time = time;
		this.newsFrom = newsFrom;
		this.commentCount = commentCount;
		this.content = content;
		this.keywords = keywords;
		this.url = url;
	}
	
	public Page() {
	}


}
