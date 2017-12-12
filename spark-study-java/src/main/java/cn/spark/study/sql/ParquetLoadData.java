package cn.spark.study.sql;

import java.util.List;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 
 * @ClassName:  ParquetLoadData   
 * @Description:Parquet数据源之使用编程方式加载数据  
 * @author: ZZU·CJWang
 * @date:   2017年12月11日 下午11:14:06   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class ParquetLoadData {

	public static void main(String[] args) {
             SparkConf sparkConf = new SparkConf()
					                 .setAppName("parquetLoadDataApp")
					                 .set("spark.testing.memory", "805306368");
             JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
             SQLContext sqlContext = new SQLContext(sparkContext);
             // 读取Parquet文件中的数据，创建一个DataFrame
             DataFrame usersDF = sqlContext.read().parquet("hdfs://spark1:9000/users.parquet");
            // 将DataFrame注册为临时表，然后使用SQL查询需要的数据
            usersDF.registerTempTable("users");
            DataFrame userNameDF = sqlContext.sql("select name from users");
           // 对查询出来的DataFrame进行transformation操作，处理数据，然后打印出来
           List<String> userNames = userNameDF.javaRDD()
                      .map(new Function<Row, String>() {

						private static final long serialVersionUID = -7269153888639637887L;

						@Override
						public String call(Row row) throws Exception {
							return row.getString(0);
						}
						
					}).collect();
           for(String userName:userNames) {
        	   System.out.println(userName);
           } 
	}

}
