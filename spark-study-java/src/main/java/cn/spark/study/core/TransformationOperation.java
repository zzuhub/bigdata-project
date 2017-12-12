package cn.spark.study.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 
 * @ClassName:  TransformationOperation   
 * @Description:Spark算子操作   
 * @author: ZZU·CJWang
 * @date:   2017年11月11日 下午10:23:15   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
@SuppressWarnings("unused")
public class TransformationOperation {

	public static void main(String[] args) {
//       map();
//		filter();
//		flatMap();
//		groupKey();
//		reduceByKey();
//		sortByKey();
//		join();
//		cogroup();

	}
	
	private static void map() {
         //1.创建SparkConf
		 SparkConf sparkConf = new SparkConf()
				                   .setAppName("MapApp")
				                   .setMaster("local");
		 
		 //2.创建SparkContext
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		//3.创建RDD
		List<Integer> nums=Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numsRDD = sparkContext.parallelize(nums);
		
		
		 //4.map操作
		JavaRDD<Integer> newNumsRDD = numsRDD.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = -3624583346034990494L;

			@Override
			public Integer call(Integer num) throws Exception {
				return num*2;
			}
			
		});
		
		newNumsRDD.foreach(new VoidFunction<Integer>() {
			
			private static final long serialVersionUID = 5485273389774629098L;

			@Override
			public void call(Integer num) throws Exception {
				 System.out.println(num);
				 
			}
		});			
		
		
		//5.关闭sparkcontext
		sparkContext.close();
		
	}
	
	public static void filter(){
		   //1.创建SparkConf
		   SparkConf sparkConf = new SparkConf()
							         .setAppName("filterApp")
							         .setMaster("local");
		   //2.创建SparkContext
		   JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		   //3.创建RDD
		   List<Integer> nums=Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		   JavaRDD<Integer> numsRDD = sparkContext.parallelize(nums);
		   //4.算子操作
		   JavaRDD<Integer> filterNumsRDD = numsRDD.filter(new Function<Integer, Boolean>() {

					private static final long serialVersionUID = -4172718453883762813L;
		
					@Override
					public Boolean call(Integer num) throws Exception {
						return num%2==0 ;   //偶数
					}
		   });
		   filterNumsRDD.foreach(new VoidFunction<Integer>() {
			
				private static final long serialVersionUID = -6794637652745584971L;
	
				@Override
				public void call(Integer num) throws Exception {
					  System.out.println(num);
				}
		});
		  //5.环比SparkContext
		  sparkContext.close();
	}
	
	public static void flatMap(){
		
		//1.创建SparkConf
		SparkConf sparkConf = new SparkConf()
				                  .setAppName("flatMapApp")
				                  .setMaster("local");
		
		//2.创建SparkContext
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		//3.创建RDD
		List<String> lineArr = Arrays.asList("spark scala",
				                             "springcloud springboot",
				                             "dubbo dubboX",
				                             "docker vue.js") ;
		JavaRDD<String> linesRDD = sparkContext.parallelize(lineArr);
		//4.算子操作
		JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = -8727501811353083L;

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));   //返回新的单词数组
			}
			
		});
		
		wordsRDD.foreach(new VoidFunction<String>() {
			
			private static final long serialVersionUID = 1677510574333554106L;

			@Override
			public void call(String word) throws Exception {
				 System.out.println(word);
			}
		});
		
		//5.关闭SparkContext
		sparkContext.close();
		
	}
	
	public static void groupKey(){
		
		//1.创建SparkConf
		SparkConf sparkConf = new SparkConf()
								    .setAppName("groupKeyApp")
								    .setMaster("local");
		
		//2.创建SparkContext
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		//3.创建RDD
		List<Tuple2<String, Integer>> clazzScores = Arrays.asList(new Tuple2<String, Integer>("金融1班",69),
																  new Tuple2<String, Integer>("金融3班",99),
																  new Tuple2<String, Integer>("金融3班",100),
																  new Tuple2<String, Integer>("金融1班",79));
		//使用特定的并行化方式
		JavaPairRDD<String, Integer> clazzScoresRDD = sparkContext.parallelizePairs(clazzScores);
		//4.执行算子操作
		JavaPairRDD<String,Iterable<Integer>> clazzScoreInfoRDD = clazzScoresRDD.groupByKey();
		clazzScoreInfoRDD.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
			
			private static final long serialVersionUID = -775924346411798866L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> clazzScore) throws Exception {
				System.out.println("============="+clazzScore._1+"==================");
				Iterator<Integer> scoresIt = clazzScore._2.iterator();
				while(scoresIt.hasNext()){
					System.out.println(scoresIt.next());
				}
				System.out.println("================================================");
			}
		});
		
		//5.关闭Spark
		sparkContext.close();
		
		
	}
	
	
	//求和
	public static void reduceByKey(){
		
		SparkConf sparkConf = new SparkConf()
							    .setAppName("reduceByKeyApp")
							    .setMaster("local");
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		List<Tuple2<String, Integer>> clazzsScores = Arrays.asList(  new Tuple2<String, Integer>("金融1班", 60),
																	new Tuple2<String, Integer>("金融1班", 68),
																	new Tuple2<String, Integer>("金融3班", 100),
																	new Tuple2<String, Integer>("金融3班", 90));
		
		JavaPairRDD<String, Integer> clazzsScoresRDD = sparkContext.parallelizePairs(clazzsScores);
		
		JavaPairRDD<String, Integer> clazzSumScoresRDD = clazzsScoresRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = -6663496500232953851L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		clazzSumScoresRDD.foreach(new VoidFunction<Tuple2<String,Integer>>() {

			private static final long serialVersionUID = -6558408260787555929L;

			@Override
			public void call(Tuple2<String, Integer> clazzSumScore) throws Exception {
				         System.out.println(clazzSumScore._1+" : "+clazzSumScore._2);
			}
		});
		
		sparkContext.close();
		
		
	}
	
	
	public static void sortByKey(){
		
		        SparkConf sparkConf = new SparkConf()
							            .setAppName("sortByKeyApp")
							            .setMaster("local");
		        
		        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		        
		        
		        List<Tuple2<Integer, String>>  scores= Arrays.asList(new Tuple2<Integer, String>(100, "王成键"),
												        		  new Tuple2<Integer, String>(59, "刘彤"),
												        		  new Tuple2<Integer, String>(150, "李彦宏"),
												        		  new Tuple2<Integer, String>(0, "马云")) ;
		        
		        
		        
		        JavaPairRDD<Integer, String> scoresRDD = sparkContext.parallelizePairs(scores);
		        
		        
		        JavaPairRDD<Integer,String> sortScores= scoresRDD.sortByKey(false);
		        
		        sortScores.foreach(new VoidFunction<Tuple2<Integer,String>>() {
					
					private static final long serialVersionUID = -1930796379937197250L;

					@Override
					public void call(Tuple2<Integer, String> scoreInfo) throws Exception {
						  System.out.println(scoreInfo._2+":"+scoreInfo._1);
					}
				});
		        
		        
		        sparkContext.close();
		        
		
	}
	
	/**
	 * 
	 * @Title: join   
	 * @Description: 连接操作  一个学生一个成绩，通过join连接即可
	 * @param:       
	 * @return: void      
	 * @throws
	 */
	public static void join() {
		
		//1.创建SparkConf
		SparkConf sparkConf = new SparkConf()
							    .setAppName("joinApp")
							    .setMaster("local");
		
		//2.创建SparkContext
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		//3.创建RDD
		List<Tuple2<Integer,String>> students=Arrays.asList(
				                            new Tuple2<Integer,String>(1,"王成键"),
				                            new Tuple2<Integer,String>(2,"刘彤"),
				                            new Tuple2<Integer,String>(3,"姜志远"),
				                            new Tuple2<Integer,String>(4,"胡笑天"));
		
		List<Tuple2<Integer,Integer>> scores=Arrays.asList(
						                new Tuple2<Integer,Integer>(1,100),
						                new Tuple2<Integer,Integer>(2,59),
						                new Tuple2<Integer,Integer>(3,60),
						                new Tuple2<Integer,Integer>(4,20));
		
		 JavaPairRDD<Integer, String> studentsRDD = sparkContext.parallelizePairs(students);
		 JavaPairRDD<Integer, Integer> scoresRDD = sparkContext.parallelizePairs(scores);
        
		
		//4.算子操作
        JavaPairRDD<Integer, Tuple2<String, Integer>> result = studentsRDD.join(scoresRDD);
        result.foreach(new  VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
			
			private static final long serialVersionUID = -736247144812504910L;

			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> record) throws Exception {
                   System.out.print("学号："+record._1);			 	
                   System.out.print("\t");			 	
                   System.out.print("姓名："+record._2._1);			 	
                   System.out.print("\t");			 	
                   System.out.print("成绩："+record._2._2);			 	
                   System.out.println();			 	
			}
		});
		
		//5.关闭Spark
        sparkContext.close();
		
	}
	
	/**
	 * 
	 * @Title: cogroup   
	 * @Description: 连接操作  一个学生多个成绩，通过cogroup连接即可  
	 * @param:       
	 * @return: void      
	 * @throws
	 */
	public static void cogroup(){
		
		//1.创建SparkConf
		SparkConf sparkConf = new SparkConf()
							    .setAppName("cogroup")
							    .setMaster("local");
		
		//2.创建SparkContext
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		//3.创建RDD
		List<Tuple2<Integer,String>> students=Arrays.asList(
                new Tuple2<Integer,String>(1,"王成键"),
                new Tuple2<Integer,String>(2,"刘彤"),
                new Tuple2<Integer,String>(3,"姜志远"),
                new Tuple2<Integer,String>(4,"胡笑天"));

		List<Tuple2<Integer,Integer>> scores=Arrays.asList(
				            new Tuple2<Integer,Integer>(1,100),
				            new Tuple2<Integer,Integer>(1,99),
				            new Tuple2<Integer,Integer>(1,98),
				            new Tuple2<Integer,Integer>(2,97),
				            new Tuple2<Integer,Integer>(2,96),
				            new Tuple2<Integer,Integer>(3,94),
				            new Tuple2<Integer,Integer>(3,93),
				            new Tuple2<Integer,Integer>(3,92),
				            new Tuple2<Integer,Integer>(3,91),
				            new Tuple2<Integer,Integer>(4,90));
		
		JavaPairRDD<Integer, String> studentsRDD = sparkContext.parallelizePairs(students);
		
		JavaPairRDD<Integer, Integer> scoresRDD = sparkContext.parallelizePairs(scores);
		
		
		//4.算子操作
		JavaPairRDD<Integer, Tuple2<Iterable<String>,Iterable<Integer>>> resultRDD = null;
		resultRDD=studentsRDD.cogroup(scoresRDD);
	
		resultRDD.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {

			private static final long serialVersionUID = 8638459476753354857L;

			@Override
			public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> record) throws Exception {
				
				 System.out.print("序号："+record._1+"\t");
				 System.out.print("姓名："+record._2._1+"\t");
				 System.out.print("成绩："+record._2._2);
				 System.out.println();
			}
		});
		
		//5.关闭Spark
		sparkContext.close();
		
	}
	

	
	
	
	

}
