package cn.spark.study.core;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 
 * @ClassName:  SecondarySort   
 * @Description:二次排序  
 * @author: ZZU·CJWang
 * @date:   2017年11月29日 下午8:34:42   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class SecondarySort {

	  public static void main(String[] args) {
		  SparkConf sparkConf = new SparkConf().setAppName("secondarySortApp").setMaster("local");
		  JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		  JavaRDD<String> linesRDD = sparkContext.textFile("d:"+File.separator+"sort.txt");
		  JavaPairRDD<SecondorySortKey, String> pairLinesRDD = linesRDD.mapToPair(new PairFunction<String, SecondorySortKey, String>() {

			private static final long serialVersionUID = 9067048871963363796L;

			@Override
			public Tuple2<SecondorySortKey, String> call(String line) throws Exception {
				String[] words = line.split(" ");
				SecondorySortKey target=new SecondorySortKey(Integer.valueOf(words[0]), Integer.valueOf(words[1]));
				Tuple2<SecondorySortKey, String> result = new Tuple2<>(target, line);
				return result;
			}
		  });
		  JavaPairRDD<SecondorySortKey, String> newLinesRDD = pairLinesRDD.sortByKey();
		  JavaRDD<String> sortedLinesRDD = newLinesRDD.map(new Function<Tuple2<SecondorySortKey,String>, String>() {

			private static final long serialVersionUID = 8950292134572797397L;

			@Override
			public String call(Tuple2<SecondorySortKey, String> target) throws Exception {
				return target._2 ;
			}
			
		  });
		  sortedLinesRDD.foreach(new VoidFunction<String>() {
			
			private static final long serialVersionUID = 6594492330070275552L;

			@Override
			public void call(String line) throws Exception {
				 System.out.println(line);
			}
		});
		  sparkContext.close();
	  }
	
}
