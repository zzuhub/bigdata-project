package cn.spark.study.core;

import java.io.Serializable;

import scala.math.Ordered;
/**
 * 
 * @ClassName:  other   
 * @Description:定义一个二次排序的类  
 * @author: ZZU·CJWang
 * @date:   2017年11月29日 下午8:13:16   
 * @company Software College OF ZhengZhou University    
 * @email  zzuandwcj@gmail.com
 *
 */
public class SecondorySortKey implements Serializable, Ordered<SecondorySortKey> {



	private static final long serialVersionUID = 4400955999991460172L;
	
	//自定义两个需要排序的类
	private Integer first;
	
	private Integer second;
	
	
	public SecondorySortKey() {}

	public SecondorySortKey(Integer first, Integer second) {
		super();
		this.first = first;
		this.second = second;
	}
	
	public Integer getFirst() {
		return first;
	}

	public void setFirst(Integer first) {
		this.first = first;
	}

	public Integer getSecond() {
		return second;
	}

	public void setSecond(Integer second) {
		this.second = second;
	}



	/**
	 * 
	 * <p>Title: $greater</p>   
	 * <p>Description:大于的情况 </p>   
	 * @param other
	 * @return   
	 * @see Ordered#$greater(A)
	 */
	@Override
	public boolean $greater(SecondorySortKey other) {
		if(this.first>other.getFirst()) {
			return true ;
		}else if(this.first==other.getFirst() && this.second>other.getSecond()) {
			return true ;
		}
		return false    ;
	}

	/**
	 * 
	 * <p>Title: $greater$eq</p>   
	 * <p>Description:大于等于的情况 </p>   
	 * @param other
	 * @return   
	 * @see Ordered#$greater$eq(A)
	 */
	@Override
	public boolean $greater$eq(SecondorySortKey other) {
		if(this.$greater(other)) {  //大于的情况
			return true;
		}else if(this.first==other.getFirst() && this.second==other.getSecond()){
			return true;
		}
		return false;
	}

	/**
	 * 
	 * <p>Title: $less</p>   
	 * <p>Description:小于 </p>   
	 * @param other
	 * @return   
	 * @see Ordered#$less(A)
	 */
	@Override
	public boolean $less(SecondorySortKey other) {
		if(this.first<other.getFirst()) {
			return true ;
		}else if(this.first==other.first&& this.second<other.getSecond()) {
			return true ;
		}
		return false;
	}

	/**
	 * 
	 * <p>Title: $less$eq</p>   
	 * <p>Description:小于等于 </p>   
	 * @param other
	 * @return   
	 * @see Ordered#$less$eq(A)
	 */
	@Override
	public boolean $less$eq(SecondorySortKey other) {
		if(this.$less(other)) {
			return true ;
		}else if(this.first==other.getFirst() && this.second < other.getSecond()) {
			return true ;
		}
		return false;
	}

	
	@Override
	public int compare(SecondorySortKey other) {
		if(this.first-other.getFirst()!=0) {
			return this.first-other.getFirst()   ;
		}
		return this.second-other.second ;
	}

	@Override
	public int compareTo(SecondorySortKey other) {
		if(this.first-other.getFirst()!=0) {
			return this.first-other.getFirst();
		}
		return this.second-other.second;
	}

}
