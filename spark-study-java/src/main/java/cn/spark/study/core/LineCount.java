package cn.spark.study.core;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 
 * @ClassName:  LineCount   
 * @Description:统计每行出现的次数  
 * @author: ZZU·CJWang
 * @date:   2017年11月11日 下午9:30:56   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class LineCount {

	public static void main(String[] args) {
            //1.创建SparkConf
		    SparkConf sparkConf = new SparkConf()
							         .setAppName("LineCount")
							         .setMaster("local") ;
		    //2.创建SparkContext
		    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		    //3.创建RDD
		    JavaRDD<String> lines = sparkContext.textFile("d:"+File.separator+"hello.txt");
		    //4.执行算子操作
		    JavaPairRDD<String, Integer> lineCount = lines.mapToPair(new PairFunction<String, String, Integer>() {

				private static final long serialVersionUID = 224969609748765814L;

				@Override
				public Tuple2<String, Integer> call(String line) throws Exception {
					return new Tuple2<String, Integer>(line, 1);
				}
			});
		    
		    JavaPairRDD<String, Integer> linesCount = lineCount.reduceByKey(new Function2<Integer, Integer, Integer>() {
				
				private static final long serialVersionUID = -5887893403505615255L;

				@Override
				public Integer call(Integer v1, Integer v2) throws Exception {
					return v1+v2;
				}
				
			 });
		    
		    linesCount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
				
				private static final long serialVersionUID = 846690783782677517L;

				@Override
				public void call(Tuple2<String, Integer> line) throws Exception {
					   System.out.println(line._1+"  次数：  "+line._2);
				}
				
			});
		    
		    
		    //5.关闭sparkContext
		    sparkContext.close();
	}

}
