package cn.spark.study.sql;

import java.io.Serializable;
/**
 * 
 * @ClassName:  Student   
 * @Description:学生实体  
 * @author: ZZU·CJWang
 * @date:   2017年12月8日 上午12:01:09   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class Student implements Serializable {

	private static final long serialVersionUID = 2512137673750464764L;

	private Integer id ;
	
	private String name ;
	
	private Integer age ;

	
	public Student() {}

	public Student(Integer id, String name, Integer age) {
		super();
		this.id = id;
		this.name = name;
		this.age = age;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	@Override
	public String toString() {
		return "Student [id=" + id + ", name=" + name + ", age=" + age + "]";
	}
	
	
	
}
