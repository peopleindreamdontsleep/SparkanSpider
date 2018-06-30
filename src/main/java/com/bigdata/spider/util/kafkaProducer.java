package com.bigdata.spider.util;

import java.util.Properties;  
import java.util.concurrent.TimeUnit;

import com.bigdata.spider.entity.Page;

import kafka.javaapi.producer.Producer;  
import kafka.producer.KeyedMessage;  
import kafka.producer.ProducerConfig;  
import kafka.serializer.StringEncoder; 

public class kafkaProducer extends Thread{  
	  
    private String topic;  
    private Page page;
      
    public kafkaProducer(String topic,Page page){  
        super();  
        this.topic = topic; 
        this.page=page;
    }  
    @SuppressWarnings("rawtypes")
	Producer producer = createProducer();  
      
    @SuppressWarnings({ "unchecked" })
	@Override  
    public void run() {  
    	producer.send(new KeyedMessage<Integer, String>(topic,page.getUrl()+"~A" +
    			page.getCatgory()+"~A"+page.getTitle()+"~A"+page.getNewsFrom()+"~A"
			 +page.getCommentCount()+"~A"+page.getContent()+"~A"+page.getKeywords()
			 +"~A"+page.getTime()));
        	
        try {  
            TimeUnit.SECONDS.sleep(1);  
        } catch (InterruptedException e) {  
            e.printStackTrace();  
        }  
    }  
  
    @SuppressWarnings("rawtypes")
	private Producer createProducer() {  
        Properties properties = new Properties();  
        properties.put("zookeeper.connect", "192.168.148.12:2181");//声明zk  
        properties.put("serializer.class", StringEncoder.class.getName());  
        properties.put("metadata.broker.list", "192.168.148.12:9092");// 声明kafka broker  
        return new Producer<Integer, String>(new ProducerConfig(properties));  
     }
    
      
      
    public static void main(String[] args) {  
       // new kafkaProducer("testTopic").start();// 使用kafka集群中创建好的主题 test 
    }  
       
}  
