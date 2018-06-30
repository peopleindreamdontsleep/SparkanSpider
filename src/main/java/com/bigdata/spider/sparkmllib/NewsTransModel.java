package com.bigdata.spider.sparkmllib;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;  
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;

import com.bigdata.spider.util.JavaUtil;

public class NewsTransModel{
	
	public static void main(String[] args) {
		
		SparkConf conf=new SparkConf().setAppName("NewsTransform").setMaster("local");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		JavaRDD<String> textFile = sc.textFile("hdfs://192.168.148.12:8020//user/hive/warehouse/newsoutput");
		
		JavaRDD<List<String>> splitedRDD= textFile.map(new Function<String, List<String>>() {

			/**
			 * 把传入一行字符串分词 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public List<String> call(String str) throws Exception {
				String jsonParse = JavaUtil.getJsonParse(str);
				String splitWords = JavaUtil.getSplitWords(jsonParse);
				return Arrays.asList(splitWords.split(" "));
			}
		});
		HashingTF hashingTF = new HashingTF();  
		
		JavaRDD<Vector> tfidf = hashingTF.transform(splitedRDD);
		
	    int numClusters=5;
	    int numiterations=4;

	    KMeansModel model = KMeans.train(tfidf.rdd(),numClusters,numiterations);
	    model.save(sc.sc(), "hdfs://192.168.148.12:8020//machine-learning/sparkmodel2");
	    //保存数据和标签
	    textFile.map(new Function<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String str) throws Exception {
				String jsonParse = JavaUtil.getJsonParse(str);
				String splitWords = JavaUtil.getSplitWords(jsonParse);
				HashingTF hashingTF = new HashingTF();  
				//List<String> asList = Arrays.asList(splitWords.split(" "));
				Vector transform = hashingTF.transform(Arrays.asList(splitWords.split(" ")));
				int prediction = model.predict(transform);
				return str.substring(0,str.length()-1)+",\"label\":\""+prediction+"\"}";
			}
		}).saveAsTextFile("hdfs://192.168.148.12:8020//machine-learning/sparkresult2");
	    
	    
		sc.close();
	}

}
