package cn.spark.study.sql;

import java.io.File;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 
 * @ClassName:  RDD2DataFrameReflect   
 * @Description:使用反射的方式将RDD转换为DataFrame   
 * @author: ZZU·CJWang
 * @date:   2017年12月8日 上午12:03:29   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class RDD2DataFrameReflect {

	public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
					            .setAppName("RDD2DataFrameReflectApp")
					            .set("spark.testing.memory", "805306368")
					            .setMaster("local");
        
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        
        SQLContext sqlContext = new SQLContext(sparkContext);
        
        JavaRDD<String> linesRDD = sparkContext.textFile("d:"+File.separator+"students.txt");
        
        JavaRDD<Student> studentsRDD = linesRDD.map(new Function<String, Student>() {

			private static final long serialVersionUID = -1367346012806146282L;

			@Override
			public Student call(String line) throws Exception {
				String[] pros = line.split(",");
				return new Student(Integer.parseInt(pros[0]),   //id
								   pros[1],  //name
								   Integer.parseInt(pros[2]));   //age
			}
		});
        
	     // 使用反射方式，将RDD转换为DataFrame
		// 将Student.class传入进去，其实就是用反射的方式来创建DataFrame
		// 因为Student.class本身就是反射的一个应用
		// 然后底层还得通过对Student Class进行反射，来获取其中的field
		// 这里要求，JavaBean必须实现Serializable接口，是可序列化的
        DataFrame studentDf = sqlContext.createDataFrame(studentsRDD, Student.class);
        
        // 拿到了一个DataFrame之后，就可以将其注册为一个临时表，然后针对其中的数据执行SQL语句
        studentDf.registerTempTable("students");
        
        DataFrame teenDf = sqlContext.sql("select * from students where age <= 18");
        
        JavaRDD<Row> teenRDD = teenDf.javaRDD();
        
        JavaRDD<Student> teeStudentRDD = teenRDD.map(new Function<Row, Student>() {

			private static final long serialVersionUID = 294283909286869738L;

			@Override
			public Student call(Row row) throws Exception {
				// row中的数据的顺序，可能是跟我们期望的是不一样的！
				return new Student(row.getInt(1),row.getString(2),row.getInt(0));
				
			}
		});
        
        List<Student> students = teeStudentRDD.collect();
        for(Student student: students) {
        	System.out.println(student);
        }
        
        
	}

}
