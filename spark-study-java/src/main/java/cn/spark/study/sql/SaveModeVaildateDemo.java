package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * 
 * @ClassName:  SaveModeVaildateDemo   
 * @Description:验证SaveMode
 * @author: ZZU·CJWang
 * @date:   2017年12月9日 上午12:37:27   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class SaveModeVaildateDemo {

	public static void main(String[] args) {
               SparkConf sparkConf = new SparkConf()
						                   .setAppName("SaveModeVaildateDemoApp")
						                   .set("spark.testing.memory", "805306368");
               
               JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
               
               SQLContext sqlContext = new SQLContext(sparkContext);
               
               DataFrame dataFrame = sqlContext.read()
						                        .format("json")
						                        .load("hdfs://spark1:9000/people.json");
               
               //如果存在就忽略，不操作
               //dataFrame.save("hdfs://spark1:9000/people_savemode_vaild", "json", SaveMode.Ignore);
               //覆盖之前的内容
               //dataFrame.save("hdfs://spark1:9000/people_savemode_vaild", "json", SaveMode.Overwrite);
               //向目录追加内容
               //dataFrame.save("hdfs://spark1:9000/people_savemode_vaild", "json", SaveMode.Append);
               //默认的，如果存在报错        
               dataFrame.save("hdfs://spark1:9000/people_savemode_vaild", "json", SaveMode.ErrorIfExists);
               
               
                   
	}

}
