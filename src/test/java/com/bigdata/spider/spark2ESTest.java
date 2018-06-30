package com.bigdata.spider;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.google.common.collect.ImmutableList;

public class spark2ESTest {
	
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("NewsKafkaParse").setMaster("local");
	    
	    sparkConf.set("es.index.auto.create", "true");
	    sparkConf.set("es.nodes", "127.0.0.1");
	    
	    JavaSparkContext sc=new JavaSparkContext(sparkConf);
	    
	    Map<String,String> esnumbersValues=new HashMap<String,String>();
	    Map<String,String> esnairportsValues=new HashMap<String,String>();
	    esnumbersValues.put("one", "1");
	    esnumbersValues.put("one", "1");
	    esnumbersValues.put("one", "1");
	    esnairportsValues.put("OTP", "Otopeni");
	    esnairportsValues.put("SFO", "San Fran");
	    
	    JavaRDD<Map<String, String>> javaRDD = sc.parallelize(ImmutableList.of(esnumbersValues, esnairportsValues));
	    JavaEsSpark.saveToEs(javaRDD, "spark/docs");
	    
	    sc.close();
	}

}
