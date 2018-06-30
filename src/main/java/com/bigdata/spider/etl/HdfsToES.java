package com.bigdata.spider.etl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Created by bee on 4/1/17.
 */
public class HdfsToES{
	
	public static class MyMapper extends Mapper<Object, Text, NullWritable,
    BytesWritable> {
		
	    public static int i=0;
	    public static int filternum=0;
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
						byte[] line = a.toString().getBytes();
				        BytesWritable blog = new BytesWritable(line);
				        context.write(NullWritable.get(), blog);
					}
				} catch (JSONException e) {
					i++;
					System.out.println("这条数据出错啦"+value.toString());
					
				}
			}else{
				filternum++;
				System.out.println("这条数据过滤啦"+value.toString());
			}
			System.out.println("总共出错"+i+"条");
			System.out.println("过滤了"+filternum+"条");
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	
	    Configuration conf = new Configuration();
	    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
	    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
	    conf.set("es.nodes", "127.0.0.1:9200");
	    conf.set("es.resource", "sina/news");
	    conf.set("es.mapping.id", "url");
	    conf.set("es.input.json", "yes");
	
	    Job job = Job.getInstance(conf, "hadoop es write test");
	    job.setMapperClass(HdfsToES.MyMapper.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(EsOutputFormat.class);
	    job.setMapOutputKeyClass(NullWritable.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	
	    // 设置输入路径
	    FileInputFormat.addInputPath(job, new Path
	            ("hdfs://192.168.148.12:8020//user/hive/warehouse/news/2017063005"));
	    job.waitForCompletion(true);
	}
	
	public static JSONObject string2json(String str) throws JSONException {  
	    String[] split = str.split("\\|");
	    int length = split.length;
	    System.out.println(length);
	    String dataJson="{\"url\":\""+split[0]+"\",\"catgory\":\""+split[1]+
	    		"\",\"title\":\""+split[2]+"\",\"newsFromandcomment\":\""+split[3]+
	    		"\",\"content\":\""+split[4]+"\",\"keywords\":\""+split[5]+
	    		"\",\"time\":\""+split[6]+"\"}";
	    JSONObject a = new JSONObject(dataJson);
	    return a;  
	} 
	
	public static void main1(String[] args) throws JSONException {
		String url="http://news.sina.com.cn/s/wh/2016-08-25/doc-ifxvixeq0408347.shtml~A ~A ~A新浪~A8818~A高考前，徐玉玉在教室学习。受访者供图。 8月6日，徐玉玉把录取通知书图片上传到QQ空间。 2016年3月27日，在教室里好友们为徐玉玉过18岁生日。受访者供图。 骗子提供给蔡芹（化名）的“通缉令”。 徐玉玉和好友在一起。受访者供图。 　　钱刚汇出，密集的雨点落了下来。徐玉玉开始等待。半小时过去，钱没有回来。她再回拨过去，“电话已经关机”。 　　从派出所走出来，她迈上了父亲的灰色金鹏电动三轮车。徐连彬发动车，走了三分钟，他想起刚下过雨，怕女儿着凉，“想叮嘱她穿上外套”。 　　一回头，发现坐在马扎上的徐玉玉，已经歪倒在车里。徐连彬停车去抱女儿，“身子都软了。”120赶到时，“人都快不行了。” 　　文|新京报记者李兴丽 张维 王煜 实习生 张笛扬 曹慧茹 龚晨霞 　　编辑 | 苏晓明 　　母亲李自云想起女儿就哭，父亲徐连彬忍着悲伤接受一波又一波的媒体采访，“我们孩子太老实了，希望其他家庭不要再受骗。” 　　8月19日，18岁的临沂罗庄女孩徐玉玉，接到了一通诈骗电话。即将踏入大学的她被骗走上大学的费用9900元。在报警回家的路上，她突然心脏骤停，经医院抢救无效，不幸于21日离世。 　　剥洋葱（微信ID：boyangcongpeople）调查发现，同一时期，接到类似诈骗电话的学生并非徐玉玉一人。与徐玉玉同一乡镇的校友也险遭诈骗；而在临沂市河东区的一个村庄，一名学生被骗走6800元学费。徐玉玉的多位同学表示，他们和家人都曾接到过推销或诈骗电话。 　　徐玉玉的去世引发了社会对个人信息泄露和电话诈骗的讨论。全国人大代表、南京邮电大学校长杨震，曾在两会上建议尽早启动个人信息保护法立法。而徐玉玉，今年考上的正是南京邮电大学。 　　等待开学的“大学生” 　　刚满18岁的徐玉玉出生在临沂市罗庄区高都街道中坦社区。姐姐徐林中国海洋大学本科毕业后，几经努力，去了新加坡的化工厂工作。 　　在中坦社区，徐玉玉是徐家第二个有出息的孩子。她入读的是临沂第十九中学的文科实验班，成绩常年保持在班级前五名，甚至一度冲上全市前20名。 　　在班主任潘宝建的印象中，徐玉玉是英语课代表，一心想学小语种，“如果正常水平发挥，最少是山东大学。” 　　今年高考，发挥不算如意的徐玉玉以568分的成绩，被南京邮电大学英语专业录取。8月6日，她在QQ空间晒出录取通知书和学校的手绘地图。“你是我（身边）最争气的同学，加油啊！”同学留言。 　　“一定会的！”徐玉玉回复。 　　入校报到的时间定在9月1日。徐连彬从半年前就开始为女儿的学费发愁。妻子李自云腿部残疾，全家开销都靠他一人在村子附近的建筑工地打零工。 　　在徐玉玉的多位同学看来，勤奋、俭省，是她身上挥之不去的标签。 　　作为一名住校生，徐玉玉每月的开销只有200元左右。同学蒋奇透露，学校食堂一荤一素，价格通常在5元左右。“徐玉玉常常只点素菜，或者光吃馒头。” 　　每年过年的压岁钱她也攒了起来。 　　临近开学，徐连彬凑了半年，8000多的学费“还是没攒够”。 　　8月17号，他带着女儿到区教育局办理了针对贫困学生的助学金申请。隔天，接到教育局电话，“说钱过几天就能发下来。” 　　按照南京邮电大学录取通知书中的要求，学生需要在20号之前把学费打进学校寄送的银行卡里，学校会自动扣款。 　　8月18号，徐连彬借了亲戚一部分钱，凑足了1万块带着女儿去银行存了：“学费8000多，剩下的是生活费，不到2000。”怕女儿去了南京不够花，徐连彬又宽慰她，“等爸爸发了工资，再给你打点生活费。” 　　19号下午4点半，骗子的电话直接打到了徐玉玉母亲李自云的手机上。听说是要“发放什么助学金”，她喊了女儿来接电话。 　　入秋后的临沂，午后天边挂着乌云。徐玉玉说，快下雨了明天再去领。 　　“19日是发放助学金的最后一天，晚了就拿不到了。”对方说，20分钟内赶到ATM机旁，通过ATM机就能领到2600元助学金。 　　李自云向剥洋葱（微信ID：boyangcongpeople）回忆，为了拿到2600元助学金，给爸爸减轻负担，徐玉玉没有犹疑，“抓起家里的雨披，骑着车就去了银行。” 　　被欺诈电话包围 　　离家3里之外就是建设银行的ATM机。按照电话里的要求，徐玉玉插了3次卡，没有取到助学金。 　　乌云在天边聚集，天黑了下来。因为此前一天接到的教育局电话，徐玉玉并没有怀疑。按照电话里的指示，她又换到了附近的农业银行。 　　对方问她身上是否有其他银行卡。徐玉玉提到了刚刚存入1万元学费的银行卡。 　　“那张交学费的银行卡还未激活，”对方要求她通过ATM机取出9900元，把钱汇入指定的账号，以“激活银行卡”，并声称半小时内，会把9900元连同2600元助学金一起汇回来，徐玉玉没有怀疑就转出了9900元。 　　钱刚汇出，密集的雨点落了下来。徐玉玉开始等待。半小时过去，钱没有回来。她再回拨过去，“电话已经关机”。 　　冒着雨，徐玉玉骑着自行车飞奔回家。“妈，我可能被骗了学费。”徐玉玉跟妈妈边分析边哭。李自云看女儿哭得厉害，劝解她，“全当花钱买了个教训。” 　　徐玉玉接受不了本就贫困的家庭横遭骗局，她拉着父亲去了派出所。 　　徐玉玉不知道，几乎同时，家住临沂市河东区汤头镇的蔡芹也被骗走了6800元学费。 　　“没有人会觉得我可怜，一切都是我自找的，怪就怪自己笨，因为自己的笨，伤害了一个家庭。”8月19日晚，从派出所回到家的蔡芹在微博上发泄心里的自责。 　　一名自称“上海市嘉定分局刑侦队”的警官称，蔡芹涉及一起非法洗钱案，正在被通缉，“如果没证据证明合法的资金流动，要在监狱里度过后半生。” 　　同样，对方称，30分钟彻查结束后会把钱退回。被吓慌的蔡芹通过支付宝汇出了银行卡里的6800元学费。 　　多方信源显示，接到诈骗电话的，并非个例。 　　“徐玉玉接到诈骗电话的那几天，也有其他班级的学生的爸爸接到类似电话。”潘宝建对剥洋葱（微信ID：boyangcongpeople）说，对方自称是临沂市财政局，通知他们领取2680块的补助。 　　幸运的是，这位同学的爸爸警惕性很高，让对方提供财政局的固定电话。对方没给。他又联系了班主任，发现“根本没有这回事。” 　　幸运没有降临在徐玉玉身上。 　　从派出所走出来，她迈上了父亲的灰色金鹏电动三轮车。徐连彬发动车，走了三分钟，他想起刚下过雨，怕女儿着凉，“想叮嘱她穿上外套”。 　　一回头，发现坐在马扎上的徐玉玉，已经歪倒在车里。徐连彬停车去抱女儿，“身子都软了。”120赶到时，“人都快不行了。” 　　我们的信息是怎么泄露的？ 　　19日晚，在临沂市矿务局医院，徐连彬被告知，女儿“已经开始脑死亡”。 　　经过抢救，徐玉玉心跳恢复，“但心脏已经供不上血，依靠呼吸机维持生命。”赶来看望徐玉玉的同学郭秋彤隔着重症监护室的玻璃，看到昔日的好友因为呼吸机压迫，“脸都变形了，脸色很难看”。 　　在班级QQ群里，跟在医院的学生随时报告徐玉玉的状况。学校也紧急给全校学生发了短信，“警惕类似的骗局”。 　　学生的不安并没有减少。“我们的信息是怎么泄露的？”、“为什么她需要助学金，骗子就找来了”、“很多推销电话，知道的（信息）还不少”…… 　　郭秋彤和同学仔细回忆了可能泄露信息的节点。“高考报名、不作弊保证书、十条警戒线，都填了家长的手机号。”日常学习中，老师为了方便管理学生，还通过飞信，加了部分学生家长。 　　8月21日晚，同学们的疑惑未及解答，QQ群里传来语音：“告诉大家一个不好的消息，玉玉已经心跳停止了。” 　　此后，“女孩被骗光学费离世”的消息经转发，引发逾千万阅读，与之相关的电信诈骗也再次被关注。 　　独立电信分析师付亮向剥洋葱（微信ID：boyangcongpeople）介绍，随着沟通的工具越来越方便，电信诈骗的方式越来越多，“防不胜防”。以经济活动的浙江省为例，公开报道显示，2015年一年，浙江警方受理通讯网络诈骗案件10.71万多起，损失高达15.43亿元，同比分别上升了41.3%和32.8%。 　　一位不愿具名的基层民警表示，徐玉玉被骗的170和171是虚拟号段，“不用身份证就可以办到”，通过手机号码追踪破案的可能很小。 　　剥洋葱（微信ID：boyangcongpeople）注意到，工业和信息化部下发的《关于贯彻落实<反恐怖主义法>等法律规定进一步做好电话用户真实身份信息登记工作的通知》，要求电信企业从严落实对入网用户实名登记工作。 　　与此同时，工信部网安局的一份调查结果显示，目前仍有不少虚拟运营商为了开拓市场，在贯彻执行政策实名制时存在违规行为，这一定程度上为骚扰信息、垃圾信息、诈骗信息泛滥提供了土壤。 　　“它涉及警方、电信运营商和银行系统”，通信行业门户网站飞象网总裁项立刚称，目前电信诈骗破案难，除了涉及公安、通信、银行等多部门，“诈骗犯效率高，成本低，而警方的效率偏低，没有形成超出区域管理的平台，我认为是大问题。” 　　项立刚举例称，按照警方属地管理的原则，山东临沂警方想要破案的话，“去广东、广西，花费的成本早就超过被骗的9000多元了。” 　　警告 　　徐玉玉去世后，徐连彬一家从她的衣物中找到了她攒的压岁钱，“有一千多块。” 　　按照计划，8月31日，徐连彬和妻子带着徐玉玉将从临沂出发到南京。在那里，他们将与从新加坡赶来的大女儿徐林汇合，送徐玉玉迈入大学。 　　5月底，姐姐徐林回了趟老家。趁着考前放假，她带着妹妹去了附近的家具城，花1500块钱买了一套衣柜和书桌，“想让她高考完，把衣物收拾出来，开学时可以直接带走。” 　　8月22日，徐玉玉收拾好的行囊被带进了火葬场。原来两姐妹挤着睡觉的屋子“完全变了模样”。 　　“20日我局立案侦查此案，目前，省厅和市局派出专人指挥、督导该案，刑警大队也参与其中。”8月24日，临沂市公安局罗庄分局相关负责人向新京报透露，此前也处理过电信诈骗案件，但像这次的情况还是第一次遇到，“非常令人痛恨，我们一定会尽全力破案。” 　　剥洋葱（微信ID：boyangcongpeople）梳理发现，根据今年2月国务院打击治理电信网络新型违法犯罪工作部际联席会议的部署，公安部刑侦局依托“电信诈骗案件侦办平台”，对全国电信诈骗涉案账户实行快速接警止付。 　　设在公安部刑侦局的全国打击治理电信网络新型违法犯罪专项行动办公室实时审核各地接警录入侦办平台的涉案账户信息，并与相关银行紧密协作，开展紧急止付工作。 　　公安、通信运营商、银行系统的联合成为各地成立反诈骗中心的一致做法。电信诈骗频发的温州市，请来了包括工农建等银行的5位资金分析专家，电信、移动、联通等通信运营商的3位通信分析专家，以及百度、阿里的5位大数据分析专家。 　　“中心成立后的一个月，成功截留了475万余元。”浙江公安官网显示，温州同期总报警金额为1400万元，这等于挽回了超过三分之一的损失。 　　8月24日上午，另一名被骗学生蔡芹接到了派出所所长打来的两通电话。“前一通说案子很受重视。后一通说，我可以申请贫困补助。”但是被骗的钱是否能追回，没有答复。 　　同日，教育部发布提醒称，广大学生尤其是大学新生，无论是哪个单位或者个人提供资助，不应要求学生到ATM机或网上进行双向互动操作。 　　临沂公安官方微博也公布了具有针对性的提醒：“当前，各大院校陆续开学，诈骗分子蠢蠢欲动。”提醒称，作案分子在电话里通知受害人按照规定可以向其发放助学金，再以需先存钱激活银行卡等为由诱骗当事人通过银行转账汇款，从而实施诈骗。 　　（文中徐林和蔡芹为化名） 责任编辑：瞿崑 SN117~A ~A ";
		String[] split = url.split("~A");
	    int length = split.length;
	    System.out.println(length);
	}

}
