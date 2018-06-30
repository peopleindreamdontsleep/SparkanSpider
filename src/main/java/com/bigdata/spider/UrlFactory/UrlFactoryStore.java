package com.bigdata.spider.UrlFactory;

import java.util.HashSet;
import java.util.Set;

import com.bigdata.spider.constant.*;
import com.bigdata.spider.service.impl.RedisRepositoryServiceImpl;
import com.bigdata.spider.util.RedisUtil;
import com.bigdata.spider.util.UrlUtil;

/*import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;*/

public class UrlFactoryStore {
	
	static RedisUtil redisutil=new RedisUtil();
	
	
	static RedisRepositoryServiceImpl redisStore=new RedisRepositoryServiceImpl();
	@SuppressWarnings("unused")
	public static Set<String> getUrl(Set<String> urllist){
		Set<String> milUrlList = new HashSet<String>();
		if(urllist!=null){
			for (String string : urllist) {
				
				
				milUrlList.addAll(UrlUtil.getNewsUrlList(string));
				if(milUrlList==null){
					continue;
				}
			}
			for (String string2 : milUrlList) {
				redisutil.add(RedisUtil.tmpWebsiteurl, string2);
				
				urlStore(string2);
			}
			//getUrl(milUrlList);
		}
		return milUrlList;
	}
	
	public static Set<String> getFatherUrl(){
		
		Set<String> fatherUrl=new HashSet<String>();
		fatherUrl.add(UrlConstant.NEWS_URL);
		fatherUrl.add(UrlConstant.EDU_URL);
		fatherUrl.add(UrlConstant.ENT_URL);
		fatherUrl.add(UrlConstant.FINANCE_URL);
		fatherUrl.add(UrlConstant.GAME_URL);
		fatherUrl.add(UrlConstant.SH_URL);
		fatherUrl.add(UrlConstant.SPORTS_URL);
		fatherUrl.add(UrlConstant.TECH_URL);
		fatherUrl.add(UrlConstant.TRAVEL_URL);
		
		return fatherUrl;
	}
	
	public static void urlStore(String url){
		
		redisStore.addWebsiteurl(url);

		 /*
		  * 集群模式下的url去重
		  * JedisCluster jedisCluster = RedisUtils.getJedisCluster(
	                new HostAndPort("192.168.2.241", 16389),
	                new HostAndPort("192.168.2.242", 16389),
	                new HostAndPort("192.168.2.243", 16389),
	                new HostAndPort("192.168.2.245", 16389),
	                new HostAndPort("192.168.2.246", 16389)
	        );


        BloomFilter<String> bloomFilter = new BloomFilter<String>(0.000001, (int)(173070*1.5));
        bloomFilter.bind(jedisCluster, "redisbitname");
        if (!bloomFilter.contains(url)) {
        	bloomFilter.add(url);
        }*/
	}
	
	public static void main(String[] args) {
	}
}
