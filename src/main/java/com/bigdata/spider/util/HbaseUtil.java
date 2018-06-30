package com.bigdata.spider.util;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.bigdata.spider.entity.Page;


@SuppressWarnings("deprecation")
public class HbaseUtil {
	/**
	 * HBASE 表名称
	 */
	public static final String TABLE_NAME = "youku";
	/**
	 * 列簇1
	 */
	public static final String COLUMNFAMILY_1 = "tudouinfo";
	/**
	 * 列簇1中的列
	 */
	public static final String COLUMNFAMILY_1_TVNAME = "tvname";
	public static final String COLUMNFAMILY_1_URL = "url";
	public static final String COLUMNFAMILY_1_ALLNUMBER = "allnumber";
	public static final String COLUMNFAMILY_1_DAYNUMBER = "daynumber";
	public static final String COLUMNFAMILY_1_COMMENTNUMBER = "commentnumber";
	public static final String COLUMNFAMILY_1_COLLECTNUMBER = "collectnumber";
	public static final String COLUMNFAMILY_1_SUPPORTNUMBER = "supportnumber";
	public static final String COLUMNFAMILY_1_AGAINSTNUMBER = "againstnumber";
	/**
	 * 列簇2
	 */
	public static final String COLUMNFAMILY_2 = "episode";

	/**
	 * 列簇2中的列
	 */
	public static final String COLUMNFAMILY_2_EPISODENUMBER = "episodenumber";
	
	Configuration conf=null;
	HBaseAdmin admin=null;
	HTablePool hTablePool = null;
	
	public HbaseUtil(){
		//读取配置文件
		conf=new  Configuration();
		conf.set("hbase.zookeeper.quorum", "hive:2181");
		conf.set("hbase.rootdir", "hdfs://hive:8020/hbase");
		
		try {
			admin=new HBaseAdmin(conf);
			hTablePool=new HTablePool(conf,1000);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*public static void main(String[] args) {
		HbaseUtil hb=new HbaseUtil();
		Page pa=hb.get("tudou","tudou_yqKF7XYmWmU");
		System.out.println(pa.getTvname());
	}*/
	
	
	/**
	 * 加入一条数据的方式
	 * @param tableName
	 * @param row
	 * @param columnFamily
	 * @param coloumn
	 * @param data
	 */
	public void put(String tableName,String row,String columnFamily,String coloumn,String data){
		HTableInterface table=hTablePool.getTable(tableName);
		
		Put p1=new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily),Bytes.toBytes(coloumn),Bytes.toBytes(data));
	
		try {
			table.put(p1);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 添加一条数据
	 * @param tableName
	 * @param row
	 * @param values
	 * @throws IOException 
	 */
	public void put(String tableName, String row, Map<String,Map<String,String>> values) throws IOException {
		HTableInterface table = hTablePool.getTable(tableName);
		Put p1 = new Put(Bytes.toBytes(row));
		for (Map.Entry<String, Map<String, String>> entry1: values.entrySet()) {
			String family = entry1.getKey();
			for (Map.Entry<String,String> entry2 : entry1.getValue().entrySet()) {
				String column = entry2.getKey();
				String value = entry2.getValue();
				if (value != null) {
					p1.add(Bytes.toBytes(family), Bytes.toBytes(column),
						Bytes.toBytes(value));
				}
			}
		}
		table.put(p1);
	}
	
	/*public Page get(String tableName, String row) {
		@SuppressWarnings("resource")
		HTablePool hTablePool = new HTablePool(conf, 1000);
		HTableInterface table = hTablePool.getTable(tableName);
		Get get = new Get(row.getBytes());
		Page page = null;
		try {
			Result result = table.get(get);
			KeyValue[] raw = result.raw();
			if (raw.length >= 9) {
				page = new Page();
				page.setTvId(row);
				page.setActor(new String(raw[0].getValue()));
				page.setAgainstnumber(new String(raw[1].getValue()));
				page.setAllnumber(new String(raw[2].getValue()));
				page.setCommentnumber(new String(raw[3].getValue()));
				page.setDirector(new String(raw[4].getValue()));
				page.setDescrib(new String(raw[5].getValue()));
				page.setSupportnumber(new String(raw[6].getValue()));
				page.setTvname(new String(raw[7].getValue()));
				page.setUrl(new String(raw[8].getValue()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return page;
	}*/
	
	public static void main(String[] args) {
		HbaseUtil hbaseutil=new HbaseUtil();
		hbaseutil.put("sina", "news", "news", "allnumber", "32342341");
	}

}


