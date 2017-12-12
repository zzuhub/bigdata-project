package cn.spark.study.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * 
 * @ClassName:  AccumulatorVariable   
 * @Description:Spark提供的Accumulator，主要用于多个节点对一个变量进行共享性的操作
 * Accumulator只提供了累加的功能。但是确给我们提供了多个task对一个变量并行操作的功能。
 * 但是task只能对Accumulator进行累加操作，不能读取它的值。
 * 只有Driver程序可以读取Accumulator的值。
 *   
 * @author: ZZU·CJWang
 * @date:   2017年11月28日 下午11:48:50   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class AccumulatorVariable {

	public static void main(String[] args) {
		     SparkConf sparkConf = new SparkConf().setAppName("accumulatorApp").setMaster("local");
		     JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		     List<Integer> nums=Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		     JavaRDD<Integer> numsRDD = sparkContext.parallelize(nums);
		     final Accumulator<Integer> sum = sparkContext.accumulator(0);   //共享累加变量
		     numsRDD.foreach(new VoidFunction<Integer>() {
				
				private static final long serialVersionUID = 2720504267697447598L;

				@Override
				public void call(Integer num) throws Exception {
					sum.add(num);   //累加操作
				}
			});
		     System.out.println("sum:"+sum.value());
		     sparkContext.close();
	  }
	
}
