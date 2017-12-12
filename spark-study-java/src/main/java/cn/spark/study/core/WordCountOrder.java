package cn.spark.study.core;

import java.io.File;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 *
 * @ClassName:  WordCountOrder
 * @Description:统计单词个数之后进行排序
 * @author: ZZU·CJWang
 * @date:   2017年11月29日 上午12:36:09
 * @company Software College OF ZhengZhou University
 * @email  zzuandwcj@gmail.com
 *
 */
public class WordCountOrder {

	public static void main(String[] args) {
              SparkConf sparkConf = new SparkConf().setAppName("worderCountOrderApp").setMaster("local");
              JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
              JavaRDD<String> linesRDD = sparkContext.textFile("d:"+File.separator+"spark.txt"); //拿到每行
              JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {

				private static final long serialVersionUID = -1284376376641931810L;

				@Override
				public Iterable<String> call(String str) throws Exception {
					return Arrays.asList(str.split(" "));
				}
			 });
             JavaPairRDD<String, Integer> wordParisRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {

				private static final long serialVersionUID = -2651081214424071556L;

				@Override
				public Tuple2<String, Integer> call(String word) throws Exception {
					return new Tuple2<String, Integer>(word,1);
				}
			}) ;
            JavaPairRDD<String, Integer> wordCountsRDD = wordParisRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
				
				private static final long serialVersionUID = -6892199142446934524L;

				@Override
				public Integer call(Integer v1, Integer v2) throws Exception {
					return v1+v2;
				}
				
			}); 
            JavaPairRDD<Integer, String> revWordCountRDD = wordCountsRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

				private static final long serialVersionUID = -5053535773623854193L;

				@Override
				public Tuple2<Integer, String> call(Tuple2<String, Integer> pair) throws Exception {
					return new Tuple2<Integer, String>(pair._2,pair._1);
				}
				
				
			});  //反转之后的单词和计数
            JavaPairRDD<Integer, String> descRevWordCountsRDD = revWordCountRDD.sortByKey(false);//降序排列
            JavaPairRDD<String, Integer> orderRDD = descRevWordCountsRDD.mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {

				private static final long serialVersionUID = -6415685688013442748L;

				@Override
				public Tuple2<String, Integer> call(Tuple2<Integer, String> pair) throws Exception {
					return new Tuple2<String, Integer>(pair._2,pair._1);
				}
			});  //反转回来
            orderRDD.foreach(new VoidFunction<Tuple2<String,Integer>>() {
				
				private static final long serialVersionUID = -1634911586747997781L;

				@Override
				public void call(Tuple2<String, Integer> pair) throws Exception {
					System.out.println("单词："+pair._1+"\t出现次数："+pair._2);
				}
			});
            
            
            sparkContext.close();
	}

}
