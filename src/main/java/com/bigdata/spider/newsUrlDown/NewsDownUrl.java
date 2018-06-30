package com.bigdata.spider.newsUrlDown;

import com.bigdata.spider.DownThread.DownloadThread;

public class NewsDownUrl {
	
	public static void main(String[] args) {
		
		int maxindex = 5; // 设置的多线程个数，修改多少个随你
		
		DownloadThread[] d = new DownloadThread[maxindex];
		
		for (int i = 0; i < maxindex; i++) {
			
			d[i] = new DownloadThread(i);
			
			d[i].start();
		
		}
	}

}
