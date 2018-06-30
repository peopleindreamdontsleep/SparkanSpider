package com.bigdata.spider.newsUrlDown;

import com.bigdata.spider.DownThread.ParseThread;

public class NewsParseUrl {
	
	public static void main(String[] args) {
		int maxindex = 5; // 设置的多线程个数，修改多少个随你
		
		ParseThread[] d = new ParseThread[maxindex];
		
		for (int i = 0; i < maxindex; i++) {
			
			d[i] = new ParseThread(i);
			
			d[i].start();
		
		}
	}
}
