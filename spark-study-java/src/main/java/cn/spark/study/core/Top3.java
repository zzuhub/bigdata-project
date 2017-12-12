package cn.spark.study.core;

import java.io.File;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 
 * @ClassName:  Top3   
 * @Description:TopN操作 
 * @author: ZZU·CJWang
 * @date:   2017年11月29日 下午9:46:14   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class Top3 {

	 public static void main(String[] args) {
	     SparkConf sparkConf = new SparkConf().setAppName("top3Appp").setMaster("local");
	     JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
	     JavaRDD<String> topRDD = sparkContext.textFile("d:"+File.separator+"top.txt");
	     JavaPairRDD<Integer, String> pairTopRDD = topRDD.mapToPair(new PairFunction<String, Integer, String>() {

			private static final long serialVersionUID = 7011107124067146864L;

			@Override
			public Tuple2<Integer, String> call(String top) throws Exception {
				return new Tuple2<Integer, String>(Integer.valueOf(top), top);
			}
	    	 
		});
	     
	     JavaPairRDD<Integer, String> sortTopRDD = pairTopRDD.sortByKey(false);
	     
	     List<Tuple2<Integer, String>> top3s = sortTopRDD.take(3);
	     
	     for(Tuple2<Integer, String> top3:top3s) {
	    	 System.out.println(top3._2);
	     }
	     
	     sparkContext.close();
	       
	 }
	
}
