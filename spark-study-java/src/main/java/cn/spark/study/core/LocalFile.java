package cn.spark.study.core;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 
 * @ClassName:  LocalFile   
 * @Description:统计本地文本单词数量
 * @author: ZZU·CJWang
 * @date:   2017年11月9日 下午11:18:16   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class LocalFile {

	 public static void main(String[] args) {
	      SparkConf conf = new SparkConf()
	                                .setAppName("LocalFile")
	                                .setMaster("local");
	      JavaSparkContext sparkContext = new JavaSparkContext(conf);
	      JavaRDD<String> lines = sparkContext.textFile("d:"+File.separator+"spark.txt", 1);
	      JavaRDD<Integer> lineCounts = lines.map(new Function<String, Integer>() {

			private static final long serialVersionUID = 5449637660434040776L;

			@Override
			public Integer call(String line) throws Exception {
				return line.length();   //因为要去除前后空格之后统计单词量
			}
			
		  });   //统计每行单词数量
	      
	      Integer count = lineCounts.reduce(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = -6111093989806113790L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
			
		  });
	      
	      System.out.println("单词统计数量:"+count);
	      
	      sparkContext.close();
	      
	 }
	
}
