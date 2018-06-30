package com.bigdata.spider.etl;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * 将数据转化成JSON格式，方便聚类分析
 * @author 11725
 *
 */
public class HdfsToJson {
	
	public static class MyMapper extends Mapper<Object, Text, NullWritable,
    Text> {
		
	    public static int i=0;
	    public static int filternum=0;
	    public static Text etlTest=new Text();
		public void map(Object key, Text value,Context context) throws IOException, InterruptedException {
			String[] split = value.toString().replaceAll("\"", "'").split("~A");
			System.out.println(split.length);
			if(split.length==8){
				String	dataJson="{\"url\":\""+split[0]+"\",\"catgory\":\""+split[1]+
		        		"\",\"title\":\""+split[2]+"\",\"newsFrom\":\""+split[3]+
		        		"\",\"comment\":\""+split[4]+
		        		"\",\"content\":\""+split[5]+"\",\"keywords\":\""+split[6]+
		        		"\",\"time\":\""+split[7]+"\"}";
		        try {
					JSONObject a = new JSONObject(dataJson);
					//因为要聚类，所以将文章标题和内容设置为不能为空，我们是对内容进行聚类的
					if(StringUtils.isNotBlank(a.getString("content"))&& StringUtils.isNotBlank(a.getString("title"))){
						etlTest.set(a.toString());
				        context.write(NullWritable.get(), etlTest);
					}
				} catch (JSONException e) {
					
				}
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	
	    Configuration conf = new Configuration();
	    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
	    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
	
	    Job job = Job.getInstance(conf, "HdfsToJson");
	    job.setJarByClass(HdfsToJson.class);
	    job.setMapperClass(HdfsToJson.MyMapper.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setMapOutputKeyClass(NullWritable.class);
	    job.setMapOutputValueClass(Text.class);
	
	    // 设置输入路径
	    FileInputFormat.addInputPath(job, new Path
	            ("hdfs://192.168.148.12:8020//user/hive/warehouse/news/2017063005"));
	    
	    FileOutputFormat.setOutputPath(job, new Path
	    		("hdfs://192.168.148.12:8020//user/hive/warehouse/newsoutput"));
	    job.waitForCompletion(true);
	}

}
