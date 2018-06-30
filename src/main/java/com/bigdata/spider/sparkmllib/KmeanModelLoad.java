package com.bigdata.spider.sparkmllib;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;

import com.bigdata.spider.util.JavaUtil;

public class KmeanModelLoad {
	
	public static void main(String[] args) {
		
		SparkConf conf=new SparkConf().setAppName("NewsTransform").setMaster("local");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		KMeansModel loadModel = KMeansModel.load(sc.sc(), "hdfs://192.168.148.12:8020//machine-learning/sparkmodel");
		
		JavaRDD<String> textFile = sc.textFile("hdfs://192.168.148.12:8020//datatest.txt");
		
		textFile.map(new Function<String, String>() {

			/**
			 * 把传入一行字符串分词 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String str) throws Exception {
				String jsonParse = JavaUtil.getJsonParse(str);
				String splitWords = JavaUtil.getSplitWords(jsonParse);
				
				HashingTF hashingTF = new HashingTF();  
				
				Vector transform = hashingTF.transform(Arrays.asList(splitWords.split(" ")));
				
				int prediction = loadModel.predict(transform);
				String resultJson=str.substring(0,str.length()-1)+",\"label\":\""+prediction+"\"}";
				return resultJson;
			}
		}).saveAsTextFile("");
		
		sc.close();
		
	}

}
