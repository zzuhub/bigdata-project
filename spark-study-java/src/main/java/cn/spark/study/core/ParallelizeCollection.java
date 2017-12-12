package cn.spark.study.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * 
 * @ClassName:  ParallelizeCollection   
 * @Description:并行化集合创建RDD
 * 案例:累加1-10
 * @author: ZZU·CJWang
 * @date:   2017年11月9日 下午10:54:15   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class ParallelizeCollection {

	  public static void main(String[] args) {
		   //1.创建SparkConf
		   SparkConf sparkConf = new SparkConf()
				                 .setAppName("Parallelize")
				                 .setMaster("local") ;
		   //2.创建SparkContext
		   JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		   //要通过并行化集合的方式创建RDD，那么就调用SparkContext及其子类的parallelize
		   List<Integer> nums = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		   JavaRDD<Integer> numsRDD = sparkContext.parallelize(nums);
		   //3.执行算子操作
		   Integer sum = numsRDD.reduce(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 3795976434985798271L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
			
		    });
		   //4.关闭spark
		   sparkContext.close();
		   System.out.println("1-10累加求和:"+sum);
	  }
	
}
