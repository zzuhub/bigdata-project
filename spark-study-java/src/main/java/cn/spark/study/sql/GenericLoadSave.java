package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 
 * @ClassName:  GenericLoadSave   
 * @Description:通用Load Save  
 * @author: ZZU·CJWang
 * @date:   2017年12月8日 下午11:59:09   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class GenericLoadSave {

	public static void main(String[] args) {
		
		  SparkConf sparkConf = new SparkConf()
								     .setAppName("GenericLoadSaveApp")
								     .set("spark.testing.memory", "805306368");
		  JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		  SQLContext sqlContext = new SQLContext(sparkContext);
		  DataFrame dataFrame = sqlContext.read().load("hdfs://spark1:9000/users.parquet");
		  dataFrame.select("name","favorite_color")
		           .write()
		           .save("hdfs://spark1:9000/namesAndFavColors.parquet");
		
	}
	
}
