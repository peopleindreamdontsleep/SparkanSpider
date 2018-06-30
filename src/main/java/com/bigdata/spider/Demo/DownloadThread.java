package com.bigdata.spider.Demo;

import java.io.IOException;

public class DownloadThread extends Thread{

	//当前ID号 
	public int ID;
	
	public boolean exit=false;
	
	public DownloadThread(int id){
	
		ID=id;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		//super.run();
		
		DownloadFile download=new DownloadFile();
		
		while(!exit){
		
			
			//从任务列表中读取一个没有被下载的任务
			Task target=Demo.getTask();
			
			if(target!=null){
			
				//下载
				System.out.println(ID);
				try {
					
					download.downLoadFromUrl(target.imageUrl, target.filename, "c:\\images");
				    
				} catch (IOException e) {
					
					e.printStackTrace();
				}
				
			}
			else{
			
				System.out.println("我是第"+ID+"个线程，我现在没有任务");
				
				//没有任务，休息一下
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					
					e.printStackTrace();
				}
			}
			
		}
		
	}

}
