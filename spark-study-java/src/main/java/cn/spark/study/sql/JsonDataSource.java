package cn.spark.study.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/**
 * 
 * @ClassName:  JsonDataSource   
 * @Description:复杂数据源，查询成绩大于80分学生的信息  
 * @author: ZZU·CJWang
 * @date:   2017年12月12日 下午10:45:39   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class JsonDataSource {

	public static void main(String[] args) {
      SparkConf sparkConf = new SparkConf()
					          .setAppName("JsonDataSourceApp")
					          .set("spark.testing.memory", "805306368");
      JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
      SQLContext sqlContext = new SQLContext(sparkContext);
      //根据JSON文件创建DataFrame
      DataFrame studentsDF = sqlContext.read()
                                      .json("hdfs://spark1:9000/students.json");
      studentsDF.printSchema();
      studentsDF.show();
      //注册表
      studentsDF.registerTempTable("student_scores");
      //成绩DF
      DataFrame studentScoresDF = sqlContext.sql("select name,score from student_scores where score>80");
      //拿到高分学生的姓名
      List<String>  names= studentScoresDF.javaRDD()
					             .map(new Function<Row, String>() {
					
									private static final long serialVersionUID = 3135072781128174160L;
					
									@Override
									public String call(Row row) throws Exception {
										return row.getString(0);   //获取姓名
									}
								})
					            .collect();
	  //创建学生信息DataFrame
      List<String> studentInfos=new ArrayList<>();
      studentInfos.add("{\"name\":\"Leo\", \"age\":18}");  
      studentInfos.add("{\"name\":\"Marry\", \"age\":17}");  
      studentInfos.add("{\"name\":\"Jack\", \"age\":19}");
      JavaRDD<String> studentInfosRDD = sparkContext.parallelize(studentInfos);
      DataFrame studentInfosDF = sqlContext.read().json(studentInfosRDD);
      studentInfosDF.registerTempTable("student_infos");
      //拼凑sql
      StringBuilder sqlSb = new StringBuilder("select name,age from student_infos where name in (");
      for(int i=0;i<names.size();i++) {
    	  sqlSb.append("'")
    	       .append(names.get(i))
    	       .append("'");
    	  if(i<names.size()-1) {
    		  sqlSb.append(",");
    	  }
      }
      sqlSb.append(")");
      //查询出高分学生信息DF
      DataFrame goodStudentInfosDF = sqlContext.sql(sqlSb.toString());
      goodStudentInfosDF.printSchema();
      goodStudentInfosDF.show();
      //两个DF合并
      JavaPairRDD<String, Tuple2<Integer, Integer>> studentAllRDD = studentScoresDF.javaRDD()
                     .mapToPair(new PairFunction<Row, String, Integer>() {

						private static final long serialVersionUID = 1810629417810250580L;

						@Override
						public Tuple2<String, Integer> call(Row row) throws Exception {
							//返回学生姓名，成绩
							return new Tuple2<String, Integer>(row.getString(0), Integer.parseInt(String.valueOf(row.getLong(1))));
						}
					})
                     .join(goodStudentInfosDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

						private static final long serialVersionUID = 3120826439347768791L;

						@Override
						public Tuple2<String, Integer> call(Row row) throws Exception {
							//学生姓名，年龄
							return new Tuple2<String, Integer>(row.getString(0), Integer.parseInt(String.valueOf(row.getLong(1))));
						}
					})) ;
      
       //将Tuple转换为Row
      JavaRDD<Row> resultRDD = studentAllRDD.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {

		private static final long serialVersionUID = 808892041149212448L;

		@Override
		public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
			//姓名  分数 年龄  
			return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
		}
	 });
      
       //创建一份元数据
      List<StructField> structFields=new ArrayList<>();
      structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
      structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
      structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
      StructType structType = DataTypes.createStructType(structFields);
      
      //最终的DataFrame
      DataFrame dataFrame = sqlContext.createDataFrame(resultRDD, structType);
      
      //保存DataFrame
      dataFrame.write()
               .format("json")
               .save("hdfs://spark1:9000/spark-study/good-students");
      
      
      
      
      
      
	}

}
