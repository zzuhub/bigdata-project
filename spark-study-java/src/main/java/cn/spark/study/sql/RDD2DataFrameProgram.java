package cn.spark.study.sql;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 
 * @ClassName:  RDD2DataFrameProgram   
 * @Description:使用编程的方式将RDD转换为DataFrame  
 * @author: ZZU·CJWang
 * @date:   2017年12月8日 下午10:17:28   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class RDD2DataFrameProgram {

	public static void main(String[] args) {
            SparkConf sparkConf = new SparkConf()
					                .setAppName("RDD2DataFrameApp")
					                .setMaster("local");
            JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
            SQLContext sqlContext = new SQLContext(sparkContext);
            JavaRDD<String> lines = sparkContext.textFile("d:"+File.separator+"students.txt");
            // 第一步，构造出元素为Row的普通RDD
            JavaRDD<Row> rowsRDD = lines.map(new Function<String, Row>() {

				private static final long serialVersionUID = 3376197655521897296L;

				@Override
				public Row call(String line) throws Exception {
					String[] fileds = line.split(",");
					return RowFactory.create(Integer.valueOf(fileds[0]),
							                fileds[1],
							                 Integer.valueOf(fileds[2])) ;
				}
			});
          // 第二步，编程方式动态构造元数据
          List<StructField> structFields = new ArrayList<StructField>();
          structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));  
          structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));  
          structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));  
          StructType structType = DataTypes.createStructType(structFields);
          
          // 第三步，使用动态构造的元数据，将RDD转换为DataFrame 
         DataFrame dataFrame = sqlContext.createDataFrame(rowsRDD, structType);
         
         dataFrame.registerTempTable("students");
         
         DataFrame teeDf = sqlContext.sql("select * from students where age <= 18");
          
         List<Row> rows = teeDf.javaRDD().collect();
         
         for(Row row:rows) {
        	 System.out.println(row);
         }
          
            
            
	}

}
