package cn.spark.study.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * 
 * @ClassName:  BroadcastVariable   
 * @Description:广播变量，只读
 * 默认情况下，如果在一个算子的函数中使用到了某个外部的变量，
 * 那么这个变量的值会被拷贝到每个task中。此时每个task只能操作自己的那份变量副本。
 * 如果多个task想要共享某个变量，那么这种方式是做不到的。
 * Spark为此提供了两种共享变量，一种是Broadcast Variable（广播变量），
 * 另一种是Accumulator（累加变量）。Broadcast Variable会将使用到的变量，
 * 仅仅为每个节点拷贝一份，更大的用处是优化性能，减少网络传输以及内存消耗。
 * Accumulator则可以让多个task共同操作一份变量，主要可以进行累加操作。
 *   
 *   
 *   
 * @author: ZZU·CJWang
 * @date:   2017年11月28日 下午11:32:32   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class BroadcastVariable {
	
	//	Spark提供的Broadcast Variable，是只读的。并且在每个节点上只会有一份副本，
	//	而不会为每个task都拷贝一份副本。因此其最大作用，就是减少变量到各个节点的网络传输消耗，
	//	以及在各个节点上的内存消耗。此外，spark自己内部也使用了高效的广播算法来减少网络消耗。
	public static void main(String[] args) {
           SparkConf sparkConf = new SparkConf().setAppName("broadcastVariableApp").setMaster("local");
           JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
           List<Integer> nums=Arrays.asList(1,3,5,7,9);
           /**
            * 可以通过调用SparkContext的broadcast()方法，
            * 来针对某个变量创建广播变量。然后在算子的函数内，
            * 使用到广播变量时，每个节点只会拷贝一份副本了。
            * 每个节点可以使用广播变量的value()方法获取值。记住，广播变量，是只读的。
           */
           final Broadcast<Integer> broadcast = sparkContext.broadcast(2); //广播变量
           JavaRDD<Integer> numsRDD = sparkContext.parallelize(nums);
           JavaRDD<Integer> doubleNumsRDD = numsRDD.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 826429742668889148L;

			 @Override
			 public Integer call(Integer num) throws Exception {
				Integer val = broadcast.getValue();
				return num*val  ;
			 }
			 
		   });
           
           doubleNumsRDD.foreach(new VoidFunction<Integer>() {
			
			private static final long serialVersionUID = -2959691376683329206L;

			@Override
			public void call(Integer num) throws Exception {
				  System.out.println(num);
			}
		});
           
          sparkContext.close(); 
           
	}

}
