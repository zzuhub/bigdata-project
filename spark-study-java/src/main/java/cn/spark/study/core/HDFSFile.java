package cn.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 
 * @ClassName:  HDFSFile   
 * @Description:使用HDFS创建RDD,统计文本字数  
 * @author: ZZU·CJWang
 * @date:   2017年11月9日 下午11:44:37   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class HDFSFile {

	public static void main(String[] args) {
          //1.创建SpakrCong
		  SparkConf sparkConf = new SparkConf()
				                    .setAppName("HDFSFile") ;
		  
		  //2.创建SparkContext
		  JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		  
		  //3.通过hdfs文件URL创建RDD
		  JavaRDD<String> lines = sparkContext.textFile("hdfs://spark1:9000/spark.txt");
		  
		  //4.执行算子操作
		  JavaRDD<Integer> lineCounts = lines.map(new Function<String, Integer>() {

			private static final long serialVersionUID = -8821626143744392729L;

			@Override
			public Integer call(String string) throws Exception {
				return string.length();
			}

		
		 });   //统计每行文本数量
		  
		  Integer count = lineCounts.reduce(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 4949226753352549443L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
			
		   }) ;    //叠加操作
		  
		  //5关闭spark
		  sparkContext.close();   
		  
		  System.out.println("文本单词量:"+count);
	}
	
	
	
	
	

}
