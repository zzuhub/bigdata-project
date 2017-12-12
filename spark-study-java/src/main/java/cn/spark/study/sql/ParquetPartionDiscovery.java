package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 
 * @ClassName:  ParquetPartionDiscovery   
 * @Description:Parquet自动分区推断  
 * @author: ZZU·CJWang
 * @date:   2017年12月11日 下午11:54:04   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class ParquetPartionDiscovery {

	public static void main(String[] args) {
               SparkConf sparkConf = new SparkConf()
					                   .setAppName("parquetPartionDiscovery")
					                   .set("spark.testing.memory", "805306368");
               JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
               SQLContext sqlContext = new SQLContext(sparkContext);
               DataFrame usersDF = sqlContext.read()
                                             .parquet("hdfs://spark1:9000/spark-study/gender=male/country=US/users.parquet");
               usersDF.printSchema();
               usersDF.show();
	}

}
