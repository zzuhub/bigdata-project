package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 
 * @ClassName:  DataFrameOperation   
 * @Description:DataFrame操作  
 * @author: ZZU·CJWang
 * @date:   2017年12月7日 下午10:28:40   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class DataFrameOperation {

	public static void main(String[] args) {
            SparkConf sparkConf = new SparkConf()
				                .setAppName("DataFrameOperationApp")
				                .set("spark.testing.memory", "805306368");
            JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
            SQLContext sqlContext = new SQLContext(sparkContext);
            
            //创建出的DataFrame完全可以理解为一张表
            DataFrame dataFrame = sqlContext.read().json("hdfs://spark1:9000/students.json");
            
            //打印DataFrame中的所有的数据(select * from ...)
            dataFrame.show();
            
            //打印DataFrame的元数据
            dataFrame.printSchema();
            
           //查询某列的所有数据
           dataFrame.select("name").show(); 
           
           //查询某几列的数据，并队列进行计算
           dataFrame.select(dataFrame.col("name"),dataFrame.col("age").plus(1)).show();
           
           //查询某一列的值进行过滤
           dataFrame.filter(dataFrame.col("age").gt(18)).show();
           
           //根据某一列进行分组，然后聚合
           dataFrame.groupBy(dataFrame.col("age")).count().show();
            
            
	}

}
