package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 
 * @ClassName:  ManualSpecialOptions   
 * @Description:手动制定数据源类型   
 * @author: ZZU·CJWang
 * @date:   2017年12月9日 上午12:20:16   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class ManualSpecialOptions {

	public static void main(String[] args) {
            SparkConf sparkConf = new SparkConf()
					                .setAppName("ManualSpecialOptionsApp")
					                .set("spark.testing.memory", "805306368");
            
            JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

            SQLContext sqlContext = new SQLContext(sparkContext);
            
            DataFrame dataFrame = sqlContext.read()
						                     .format("json")
						                     .load("hdfs://spark1:9000/people.json");
            
            dataFrame.select("name")
                     .write()
                     .format("json")
                     .save("hdfs://spark1:9000/peopleName_java");
            
            
	}

}
