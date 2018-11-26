package com.bigdata.spider.sparkstreming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import scala.Tuple2;

import com.bigdata.spider.util.JavaUtil;
import kafka.serializer.StringDecoder;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.JSONObject;

public class NewsKafkaParse {
	
	  public static void main(String[] args) {

	    StreamingExamples.setStreamingLogLevels();

	    String brokers = "192.168.148.12:9092";
	    String topics = "testTopic";

	    // Create context with a 2 seconds batch interval
	    SparkConf sparkConf = new SparkConf().setAppName("NewsKafkaParse").setMaster("local");
	    
	    sparkConf.set("es.index.auto.create", "true");
	    sparkConf.set("es.nodes", "127.0.0.1");
	    
	    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
	    
	    KMeansModel loadModel = KMeansModel.load(jssc.sparkContext().sc(), "hdfs://192.168.148.12:8020//machine-learning/sparkmodel2");
	    
	    HashSet<String> topicsSet = new HashSet<String>();
	    topicsSet.add(topics);
	    HashMap<String, String> kafkaParams = new HashMap<String, String>();
	    kafkaParams.put("metadata.broker.list", brokers);

	    // Create direct kafka stream with brokers and topics
	    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
	        jssc,
	        String.class,
	        String.class,
	        StringDecoder.class,
	        StringDecoder.class,
	        kafkaParams,
	        topicsSet
	    );

	    // Get the lines, split them into words, count the words and print
	    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
		      public String call(Tuple2<String, String> tuple2) {
				String line=tuple2._2();
				String[] split = line.replaceAll("\"", "'").split("~A");
				if(split.length==8){
					String	dataJson="{\"url\":\""+split[0]+"\",\"catgory\":\""+split[1]+
			        		"\",\"title\":\""+split[2]+"\",\"newsFrom\":\""+split[3]+
			        		"\",\"comment\":\""+split[4]+
			        		"\",\"content\":\""+split[5]+"\",\"keywords\":\""+split[6]+
			        		"\",\"time\":\""+split[7]+"\"}";
					String jsonParse = JavaUtil.getJsonParse(dataJson);
					String splitWords = JavaUtil.getSplitWords(jsonParse);
					//存储模型的时候，最好把tf-idf的结果也存下来，这样，结果才更真确，这里，存在偏差
					HashingTF hashingTF = new HashingTF();  
					
					Vector transform = hashingTF.transform(Arrays.asList(splitWords.split(" ")));
					
					int prediction = loadModel.predict(transform);
					
					String resultJson=dataJson.substring(0,dataJson.length()-1)+",\"label\":\""+prediction+"\"}";
			        return resultJson;
				}
				return "";
		      }
		    });
	    
	    JavaDStream<String> filterRDD = lines.filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				
				return StringUtils.isNotBlank(v1);
			}
		});
	    
	    filterRDD.foreachRDD(new Function<JavaRDD<String>, Void>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaRDD<String> lineRDD) throws Exception {
				
				JavaRDD<Map<String, String>> mapRDD = lineRDD.map(new Function<String, Map<String,String>>() {

					/**
					 * 将数据写入到es中，通过map写入
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Map<String, String> call(String value) throws Exception {
						Map<String, String> esValue=new HashMap<String,String>();
						
						JSONObject str = new JSONObject(value);
						 
						esValue.put("url", str.getString("url"));
						esValue.put("catgory", str.getString("catgory"));
						esValue.put("title", str.getString("title"));
						esValue.put("newsFrom", str.getString("newsFrom"));
						esValue.put("comment", str.getString("comment"));
						esValue.put("content", str.getString("content"));
						esValue.put("keywords", str.getString("keywords"));
						esValue.put("time", str.getString("time"));
						esValue.put("label", str.getString("label"));
						return esValue;
						
					}
				});
				
				JavaEsSpark.saveToEs(mapRDD, "spark/news");
				
				mapRDD.foreach(new VoidFunction<Map<String,String>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(Map<String, String> t) throws Exception {
						for (Map.Entry<String, String> str : t.entrySet()) {
							System.out.println(str.getKey()+"==="+str.getValue());
						}
					}
				});
				
				return null;
			}
		});

	    // Start the computation'
	    jssc.start();
	    jssc.awaitTermination();
	  }
}
