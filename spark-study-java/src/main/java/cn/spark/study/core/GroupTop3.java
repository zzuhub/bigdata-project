package cn.spark.study.core;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 
 * @ClassName:  GroupTop3   
 * @Description:分组取Top3  
 * @author: ZZU·CJWang
 * @date:   2017年11月29日 下午10:19:51   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class GroupTop3 {

	public static void main(String[] args) {
               SparkConf sparkConf = new SparkConf().setAppName("groupTop3App").setMaster("local");
               JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
               JavaRDD<String> lineRDD = sparkContext.textFile("d:"+File.separator+"score.txt");
               JavaPairRDD<String, Integer> pairRDD = lineRDD.mapToPair(new PairFunction<String, String, Integer>() {

				/**   
				 * @Fields serialVersionUID : TODO(用一句话描述这个变量表示什么)   
				 */  
				private static final long serialVersionUID = -3290748378334183193L;

				@Override
				public Tuple2<String, Integer> call(String line) throws Exception {
					return new Tuple2<String, Integer>(line.split(" ")[0],Integer.valueOf(line.split(" ")[1]));
				}
				
            	   
			   });
               
               JavaPairRDD<String, Iterable<Integer>> groupRDD = pairRDD.groupByKey();
             
               JavaPairRDD<String, Iterable<Integer>> top3RDD = groupRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterable<Integer>>() {

				private static final long serialVersionUID = 765042598804823434L;

				@Override
				public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> clazzScores) throws Exception {
				    Integer[]  top3=new Integer[3];    //存取前三名成绩
				    String clazzName=clazzScores._1;  //班级名称
				    Iterator<Integer> scores = clazzScores._2.iterator();
				    while(scores.hasNext()) {
				    	Integer score=scores.next();   //当前分数
				    	for(int i=0;i<top3.length;i++) {
				    		if(top3[i]==null) {
				    			top3[i]=score;
				    			break ;
				    		}else if(score>top3[i]) {
				    			for(int j=top3.length-1;j>i;j--) {
				    				 top3[j]=top3[j-1];
				    			}
				    			top3[i]=score;
				    			break;
				    		}
				    	}
				    }
					return new Tuple2<String, Iterable<Integer>>(clazzName, Arrays.asList(top3));
				}
            	   
            	   
			   });
               
               
               top3RDD.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
				
				private static final long serialVersionUID = 6059996505734960426L;

				@Override
				public void call(Tuple2<String, Iterable<Integer>> top3) throws Exception {
					     System.out.println("班级："+top3._1);
					     Iterator<Integer> scores = top3._2.iterator();
					     while(scores.hasNext()) {
					    	 System.out.println(scores.next());
					     }
					     System.out.println("=================================");
				}
			});
               
               
               
               
             sparkContext.close();  
               
	}

}
