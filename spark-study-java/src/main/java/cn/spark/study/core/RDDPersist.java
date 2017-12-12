package cn.spark.study.core;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * @ClassName:  RDDPersist   
 * @Description:RDD持久化   
 * @author: ZZU·CJWang
 * @date:   2017年11月28日 下午10:40:24   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class RDDPersist {

	public static void main(String[] args) {
		    SparkConf sparkConf = new SparkConf().setAppName("RDDPersistApp").setMaster("local");
		    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		    JavaRDD<String> txtRDD = sparkContext.textFile("d:"+File.separator+"hello.txt").cache();
		    long start=System.currentTimeMillis();
		    long count = txtRDD.count();
		    System.out.println("计算结果："+count+"，耗时："+(System.currentTimeMillis()-start));
		    start=System.currentTimeMillis();
		    count = txtRDD.count();
		    System.out.println("计算结果："+count+"，耗时："+(System.currentTimeMillis()-start));
		    sparkContext.close();
	  }
	
}
