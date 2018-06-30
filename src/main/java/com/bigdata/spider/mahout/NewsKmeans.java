/*package com.bigdata.spider.mahout;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.hadoop.stats.BasicStats;
import org.apache.mahout.utils.io.ChunkedWriter;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.HighDFWordsPruner;
import org.apache.mahout.vectorizer.collocations.llr.LLRReducer;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.lionsoul.jcseg.tokenizer.ASegment;
import org.lionsoul.jcseg.tokenizer.core.ADictionary;
import org.lionsoul.jcseg.tokenizer.core.DictionaryFactory;
import org.lionsoul.jcseg.tokenizer.core.IWord;
import org.lionsoul.jcseg.tokenizer.core.JcsegTaskConfig;
import org.lionsoul.jcseg.tokenizer.core.SegmentFactory;
import com.google.common.io.Closeables;

@SuppressWarnings("deprecation")
public class NewsKmeans {
	private Configuration conf = null;
	private FileSystem fs = null;
	private String dataFile;
	public String outputPath;
	public final static String seqdirectoryPath = "seqdirectory";
	public final static String seq2sparsePath = "seq2sparse";
	public final static String kmeansPath = "kmeans";
	public final static String canopyPath = "canopy";
	public final static String canopyKmeansPath = "canopy_kmeans";
	public NewsKmeans(String i, String o) throws Exception {
		this.dataFile = i;
		this.outputPath = o;
		conf = new Configuration();
		fs = FileSystem.get(conf);
	}

	*//**
	 * 把指定 输入的内容转成hadoop的序列文件
	 *//*
	public void seqdirectory() {
		try {
			JcsegTaskConfig config = new JcsegTaskConfig();
			ADictionary dic = DictionaryFactory.createDefaultDictionary(config);
			ASegment seg = (ASegment) SegmentFactory.createJcseg(
					JcsegTaskConfig.COMPLEX_MODE, new Object[] { config, dic });
			Path outputDir = new Path(outputPath, seqdirectoryPath);
			fs.deleteOnExit(outputDir);
			fs.mkdirs(outputDir);
			ChunkedWriter writer = new ChunkedWriter(conf, 64, outputDir);
			FileReader fr = new FileReader(dataFile);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while ((line = br.readLine()) != null) {
				JSONObject json = (JSONObject) JSONValue.parse(line);
				String id = json.get("url").toString();
				String title = json.get("title").toString();
				String content = json.get("content").toString();
				seg.reset(new StringReader(title + " " + content));
				IWord word = null;
				StringBuffer sr = new StringBuffer();
				while ((word = seg.next()) != null) {
					String type = word.getPartSpeech() != null ? word
					.getPartSpeech()[0] : "";
					if (type.startsWith("w") == false) {
						sr.append(word.getValue()).append(" ");
					}
				}
				writer.write(id, sr.toString());
			}
			Closeables.close(writer, false);
			br.close();
			fr.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	*//**
	 * 把文本内容向量化
	 *//*
	public void seq2sparse(){
	    try {
	      Path inputDir = new Path(this.outputPath, NewsKmeans.seqdirectoryPath);
	      Path outputDir = new Path(this.outputPath, NewsKmeans.seq2sparsePath);
	      int chunkSize = 100;
	      int minSupport = 2;
	      int maxNGramSize = 1;
	      float minLLRValue = LLRReducer.DEFAULT_MIN_LLR;
	      int reduceTasks = 1;
	      Class<? extends Analyzer> analyzerClass = WhitespaceAnalyzer.class;//StandardAnalyzer.class;
	      boolean processIdf = true;
	      int minDf = 1;
	      int maxDFPercent = 99;
	      double maxDFSigma = -1.0;
	      float norm = PartialVectorMerger.NO_NORMALIZING;
	      boolean logNormalize = false;
	      Path tokenizedPath = new Path(outputDir, DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);
	      DocumentProcessor.tokenizeDocuments(inputDir, analyzerClass, tokenizedPath, conf);
	      boolean sequentialAccessOutput = true;
	      boolean namedVectors = true;
	      boolean shouldPrune = maxDFSigma >= 0.0 || maxDFPercent > 0.00;
	      String tfDirName = shouldPrune
	              ? DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER + "-toprune"
	              : DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER;
	      if (processIdf) {
	        DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath,
	                outputDir,
	                tfDirName,
	                conf,
	                minSupport,
	                maxNGramSize,
	                minLLRValue,
	                -1.0f,
	                false,
	                reduceTasks,
	                chunkSize,
	                sequentialAccessOutput,
	                namedVectors);
	      } else {
	        DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath,
	                outputDir,
	                tfDirName,
	                conf,
	                minSupport,
	                maxNGramSize,
	                minLLRValue,
	                norm,
	                logNormalize,
	                reduceTasks,
	                chunkSize,
	                sequentialAccessOutput,
	                namedVectors);
	      }
	      Pair<Long[], List<Path>> docFrequenciesFeatures = null;
	      if (shouldPrune || processIdf) {
	        docFrequenciesFeatures =
	                TFIDFConverter.calculateDF(new Path(outputDir, tfDirName), outputDir, conf, chunkSize);
	      }
	      long maxDF = maxDFPercent; //if we are pruning by std dev, then this will get changed
	      if (shouldPrune) {
	        long vectorCount = docFrequenciesFeatures.getFirst()[1];
	        if (maxDFSigma >= 0.0) {
	          Path dfDir = new Path(outputDir, TFIDFConverter.WORDCOUNT_OUTPUT_FOLDER);
	          Path stdCalcDir = new Path(outputDir, HighDFWordsPruner.STD_CALC_DIR);
	          // Calculate the standard deviation
	          double stdDev = BasicStats.stdDevForGivenMean(dfDir, stdCalcDir, 0.0, conf);
	          maxDF = (int) (100.0 * maxDFSigma * stdDev / vectorCount);
	        }
	        long maxDFThreshold = (long) (vectorCount * (maxDF / 100.0f));
	        // Prune the term frequency vectors
	        Path tfDir = new Path(outputDir, tfDirName);
	        Path prunedTFDir = new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER);
	        Path prunedPartialTFDir =
	                new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER + "-partial");
//	        log.info("Pruning");
	        if (processIdf) {
	          HighDFWordsPruner.pruneVectors(tfDir,
	                  prunedTFDir,
	                  prunedPartialTFDir,
	                  maxDFThreshold,
	                  minDf,
	                  conf,
	                  docFrequenciesFeatures,
	                  -1.0f,
	                  false,
	                  reduceTasks);
	        } else {
	          HighDFWordsPruner.pruneVectors(tfDir,
	                  prunedTFDir,
	                  prunedPartialTFDir,
	                  maxDFThreshold,
	                  minDf,
	                  conf,
	                  docFrequenciesFeatures,
	                  norm,
	                  logNormalize,
	                  reduceTasks);
	        }
	        HadoopUtil.delete(new Configuration(conf), tfDir);
	      }
	      if (processIdf) {
	        TFIDFConverter.processTfIdf(
	                new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
	                outputDir, conf, docFrequenciesFeatures, minDf, maxDF, norm, logNormalize,
	                sequentialAccessOutput, namedVectors, reduceTasks);
	      }
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	}
	
	*//**
	 * kmeans聚类分析文本
	 *//*
	public void kmeans(){
		try{
			int k = 50;
			int maxIterations = 10;
			double convergenceDelta = 0.01;
			DistanceMeasure measure = new EuclideanDistanceMeasure();
			Path initialPoints = new Path(this.outputPath, "random-seeds");
			fs.deleteOnExit(initialPoints);
			Path input = new Path(this.outputPath + "/" + NewsKmeans.seq2sparsePath, "tfidf-vectors");
			Path output = new Path(this.outputPath, NewsKmeans.kmeansPath);
			fs.deleteOnExit(output);
		    RandomSeedGenerator.buildRandom(conf, input, initialPoints, k, measure, 1L);
		    KMeansDriver.run(conf, input, initialPoints, output, convergenceDelta,
		        maxIterations, true, 0.0, false);
		    // run ClusterDumper
//		    Path outGlob = new Path(output, "clusters-*-final");
//		    Path clusteredPoints = new Path(output,"clusteredPoints");
//		    ClusterDumper clusterDumper = new ClusterDumper(outGlob, clusteredPoints);
//		    clusterDumper.printClusters(null);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	*//**
	 * 读取聚类分析结果
	 * @param input
	 * @param output
	 * @param gFile
	 *//*
	public void clusterResult(String input, String output, String gFile) {
		String file = this.outputPath + "/" + input;
		String txtFile = this.outputPath + "/" + output;
		String groupFile = this.outputPath + "/" + gFile;
		try {
			BufferedWriter bw;
			Configuration conf = new Configuration();
			// conf.set("fs.default.name", "hdfs://baby6:31054");
			FileSystem fs = FileSystem.get(conf);
			SequenceFile.Reader reader = null;
			reader = new SequenceFile.Reader(fs, new Path(file), conf);
			bw = new BufferedWriter(new FileWriter(new File(txtFile)));
			HashMap<String, Integer> clusterIds;
			clusterIds = new HashMap<String, Integer>(120);
			IntWritable key = new IntWritable();
//			WeightedVectorWritable value = new WeightedVectorWritable();
			WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
			while (reader.next(key, value)) {
				NamedVector vector = (NamedVector) value.getVector();
				String vectorName = vector.getName();
				bw.write(vectorName + "\t" + key.toString() + "\n");
				if (clusterIds.containsKey(key.toString())) {
					clusterIds.put(key.toString(),
					clusterIds.get(key.toString()) + 1);
				} else
					clusterIds.put(key.toString(), 1);
			}
			bw.flush();
			bw.close();
			reader.close();
			bw = new BufferedWriter(new FileWriter(new File(groupFile)));
			Set<String> keys = clusterIds.keySet();
			for (String k : keys) {
				bw.write(k + " " + clusterIds.get(k) + "\n");
			}
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	*//**
	 * 根据聚类结果对文章分析保存
	 * @param resultFile
	 * @param resultPath
	 *//*
	public void saveCluster(String resultFile, String resultPath) {
		try {
			String resultTxt = this.outputPath + "/" + resultFile;// "data/penngo/result.txt";
			FileReader fr = new FileReader(resultTxt);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			Map<String, String> idMap = new HashMap<String, String>();
			while ((line = br.readLine()) != null) {
				String[] strs = line.split("\t");
				// System.out.println(strs[0] + " " + strs[1]);
				idMap.put(strs[0], strs[1]);
			}
			br.close();
			fr.close();
			Map<String, BufferedWriter> writerMap = new HashMap<String, BufferedWriter>();
			FileReader frNews = new FileReader(this.dataFile);
			BufferedReader brNews = new BufferedReader(frNews);
			String lineNews = null;
			while ((lineNews = brNews.readLine()) != null) {
				JSONObject json = (JSONObject) JSONValue.parse(lineNews);
				String id = json.get("url").toString();
				String title = json.get("title").toString();
				String content = json.get("content").toString();
				String type = idMap.get(id);
				BufferedWriter bw = writerMap.get(type);
				if (bw == null) {
					String f = this.outputPath + "/result/" + resultPath;// + "/" +
					File file = new File(f);
					if (file.exists() == false) {
						file.mkdirs();
					}
					f = f + "/" + type + ".txt";
					FileWriter fw = new FileWriter(f);
					bw = new BufferedWriter(fw);
					writerMap.put(type, bw);
				}
				//bw.write(id + "    " + title + "   " + content);
				bw.write(id + "    " + title);
				bw.newLine();
			}
			Iterator<Entry<String, BufferedWriter>> it = writerMap.entrySet()
			.iterator();
			while (it.hasNext()) {
				Entry<String, BufferedWriter> e = it.next();
				e.getValue().close();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	*//**
	 * 使用canopy+kmeans算法分析文章
	 * 
	 *//*
	public void canopyKmeans() {
		try {
			EuclideanDistanceMeasure measure = new EuclideanDistanceMeasure();
			Path vectorsPath = new Path(this.outputPath + "/" + NewsKmeans.seq2sparsePath
			+ "/tfidf-vectors");
			Path canopyOutput = new Path(this.outputPath, NewsKmeans.canopyPath);
			Path outputPath = new Path(this.outputPath, NewsKmeans.canopyKmeansPath);
			CanopyDriver.run(new Configuration(), vectorsPath, canopyOutput,
					measure, 100, 150, false, 0.0,
					false);
			Path initialPoints = new Path(canopyOutput, Cluster.INITIAL_CLUSTERS_DIR + "-final");
			KMeansDriver.run(conf, vectorsPath, initialPoints, outputPath,
					1.0, 10, true, 0.0, false);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] srgs) {
//		System.setProperty("hadoop.home.dir", "D:/hadoop/hadoop-2.6.4");
//		System.setProperty("HADOOP_USER_NAME", "root");
		try{
			NewsKmeans newsInfo = new NewsKmeans("data.txt", 
					"dataoutput");
			newsInfo.seqdirectory();
			newsInfo.seq2sparse();
			
			newsInfo.kmeans();
			newsInfo.clusterResult(NewsKmeans.kmeansPath + "/clusteredPoints/part-m-00000", "result_kmeans.txt", "group_kmeans.txt");
			newsInfo.saveCluster("result_kmeans.txt", "kmeans");

			newsInfo.canopyKmeans();
			newsInfo.clusterResult(NewsKmeans.canopyKmeansPath + "/clusteredPoints/part-m-00000", "result_canopy_kmeans.txt", "group_canopy_kmeans.txt");
			newsInfo.saveCluster("result_canopy_kmeans.txt", "canopy_kmeans");
		}
		catch(Exception e){
			e.printStackTrace();
		}
		

	}
}
*/