package com.bigdata.spider.Demo;

import java.util.ArrayList;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Demo {

	/**
	 * @param args
	 */

	public static ArrayList<Task> arr = new ArrayList<Task>();

	public static void main(String[] args) {

		GeiALLimgUrl("http://www.csdn.net"); // 封装目标url
		
		int maxindex = 2; // 设置的多线程个数，修改多少个随你
		
		DownloadThread[] d = new DownloadThread[maxindex];
		
		for (int i = 0; i < maxindex; i++) {
			
			d[i] = new DownloadThread(i);
			
			d[i].start();
		
		}
	}

	public static void GeiALLimgUrl(String url) {
		try {
			String result = HttpUtils.doGet(url);
			
			Document doc = Jsoup.parse(result);
			
			Elements links = doc.select("img");
			
			for (Element imgs : links) {
				
				System.out.println(imgs.attr("src")); // 抓取的当前URL页面上的图片img
				
				arr.add(new Task(imgs.attr("src"))); // 先存放在集合里，后续再操作
			}
		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	public static Task getTask() {
		for (Task s : arr) {
			if (!s.hasDownloaded) {
				s.hasDownloaded = true;
				return s;
			}
		}
		return null;
	}
}
