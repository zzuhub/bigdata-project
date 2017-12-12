package cn.spark.study.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

/**
 * 
 * @ClassName:  ActionOperation   
 * @Description:Action操作  
 * @author: ZZU·CJWang
 * @date:   2017年11月27日 下午10:52:29   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class ActionOperation {

	public static void main(String[] args) {
//		reduce() ;
//		collect();
//		count() ;
//		take();
//		countByKey();
		saveAsTextFile();
	}
	
	public static void reduce() {
		
		//1.创建SparkConf
		SparkConf sparkConf = new SparkConf()
								   .setAppName("reduceApp")
								   .setMaster("local");
		
		//2.创建SparkContext
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		//3.创建RDD
		List<Integer> nums=Arrays.asList(1,3,5,7,9);
		JavaRDD<Integer> numsRDD = sparkContext.parallelize(nums);		
		
		//4.创建算子操作
		Integer sum = numsRDD.reduce(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 3529348914594582495L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
			
		});
		
	   System.out.println("sum:"+sum);
		
		//5.关闭spark
		sparkContext.close();
		
	}
	
	//返回集群中全部元素
	public static void collect() {
		
		SparkConf sparkConf = new SparkConf()
							    .setAppName("collectApp")
							    .setMaster("local");
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		List<Integer> nums=Arrays.asList(1,3,5,7,9);
		
		JavaRDD<Integer> numsRDD = sparkContext.parallelize(nums);
		
		List<Integer> numsList = numsRDD.collect();
		
		for(Integer num:numsList) {
			System.out.println(num);
		}
		
		sparkContext.close();
		
	}
	
	public static void count() {
		SparkConf sparkConf = new SparkConf()
							    .setAppName("countApp")
							    .setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		List<Integer> nums=Arrays.asList(1,3,5,7,9);
		JavaRDD<Integer> numsRDD = sparkContext.parallelize(nums);
		long count = numsRDD.count();
		System.out.println("count:"+count);
		sparkContext.close();
	}
	
	public static void take() {
		
		SparkConf sparkConf=new SparkConf()
							    .setAppName("takeApp")
							    .setMaster("local");
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
				    
		List<Integer> nums=Arrays.asList(1,3,5,7,9);
		
		JavaRDD<Integer> numsRDD = sparkContext.parallelize(nums);
		
		List<Integer> top3Nums = numsRDD.take(3);
		
		for(Integer top3Num:top3Nums) {
			System.out.println(top3Num);
		}
		
		sparkContext.close();
		
	}
	
	public static void countByKey() {
		
		SparkConf sparkConf = new SparkConf()
								    .setAppName("countByKeyApp")
								    .setMaster("local");
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		List<Tuple2<String, String>> students=Arrays.asList(new Tuple2<String,String>("1班","刘彤"),
															new Tuple2<String,String>("1班","黄蕾放"),      
															new Tuple2<String,String>("3班","王晴"),     
															new Tuple2<String,String>("3班","王成键"),      
															new Tuple2<String,String>("3班","胡笑天"),      
															new Tuple2<String,String>("3班","宋庚泽"));      
		
		JavaPairRDD<String, String> studentsRDD = sparkContext.parallelizePairs(students);
		
		Map<String, Object> countMap = studentsRDD.countByKey();
		
		for(Map.Entry<String, Object> countEle:countMap.entrySet()) {
			System.out.println(countEle.getKey()+"："+countEle.getValue());
		}
		
		sparkContext.close();
	}
	
	public static void saveAsTextFile() {
		 SparkConf sparkConf = new SparkConf()
				                  .setAppName("saveAsTextFile");
		 
		 JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		 
		 List<Integer> nums=Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		 
		 JavaRDD<Integer> numsRDD = sparkContext.parallelize(nums);
		 
		 JavaRDD<Integer> doubleNumsRDD = numsRDD.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = -5477989043662138147L;

			@Override
			public Integer call(Integer num) throws Exception {
				return num*2 ;
			}
		});
		
		  // 直接将rdd中的数据，保存在HFDS文件中
	     // 但是要注意，我们这里只能指定文件夹，也就是目录
	     // 那么实际上，会保存为目录中的/double_number.txt/part-00000文件
		 doubleNumsRDD.saveAsTextFile("hdfs://spark1:9000/double_number");  
		 
		 
		 sparkContext.close();
		 
		 
	}
	
	
	
	
	
	
	
	
}
