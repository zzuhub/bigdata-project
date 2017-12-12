package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 
 * @ClassName:  SqlCreate   
 * @Description:使用json文件创建DataFrame
 * @author: ZZU·CJWang
 * @date:   2017年12月6日 下午9:10:58   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class SqlCreate {

	public static void main(String[] args) {
          SparkConf sparkConf = new SparkConf()
        		                .setAppName("DataFrameCreateApp")
        		                .set("spark.testing.memory", "805306368");
          JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
          SQLContext sqlContext = new SQLContext(sparkContext);
          DataFrame dataFrame = sqlContext.read().json("hdfs://spark1:9000/students.json");
          dataFrame.show();
	}
}
