package de.jakob_kroemer;
import org.apache.spark.api.java.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Merge {
	public static JavaRDD<String> merge(String file1,String file2) {
		
	    SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    long data1count = sc.textFile(file1).count();
	    System.out.println(data1count);
	    long data2count = sc.textFile(file2).count();
	    System.out.println(data2count);
	    JavaRDD<String> data1 = sc.textFile(file1).mapPartitionsWithIndex((index, iterator) -> {
            if (index == 0 || index == data1count-1) {
                iterator.next();
            }
            return iterator;
        }, true).cache();
	    JavaRDD<String> data2 = sc.textFile(file2).mapPartitionsWithIndex((index, iterator) -> {
            if (index == 0 || index == data2count-1 ) {
                iterator.next();
            }
            return iterator;
        }, true).cache();
	    
	   
	    
	    //System.out.println("hey:"+data2.collect().get(1));
	    //create pairs with key: passengerNumber values: tipAmount
		
	    PairFunction<String, String, String> keyData = new PairFunction<String,String, String>() { 
	    	public Tuple2<String, String> call(String s) { 
	    		String[]attributes = s.split(","); // key , value 
	    		System.out.println(attributes.toString());
	    		return new Tuple2(s.split(",")[0]+":"+s.split(",")[1]+":"+s.split(",")[5],s.split(",")[2]+","+s.split(",")[3]+","+s.split(",")[4]+","+s.split(",")[5]+","
	    				+s.split(",")[6]+","+s.split(",")[7]+","+s.split(",")[8]+","+s.split(",")[9]+","+s.split(",")[10]+","
	    				+s.split(",")[11]+","+s.split(",")[12]+","+s.split(",")[13]); 
	    		} 
	    	};
	   
	    PairFunction<String, String, String> keyFare = new PairFunction<String,String, String>() { 
	    	public Tuple2<String, String> call(String s) { 
	    		//String[]attributes = s.split(","); // key , value 
	    		return new Tuple2(s.split(",")[0]+":"+s.split(",")[1]+":"+s.split(",")[3],s.split(",")[2]+","+s.split(",")[3]+","+s.split(",")[4]+","+s.split(",")[5]+","
	    		+s.split(",")[6]+","+s.split(",")[7]+","+s.split(",")[8]+","+s.split(",")[9]+","+
	    		s.split(",")[10]);
	    	}
	    };
		  
		  
		  JavaPairRDD<String,String> pairs1 = data1.mapToPair(keyData);
		  JavaPairRDD<String,String> pairs2 = data2.mapToPair(keyFare);
		  JavaPairRDD<String,Tuple2<String, String>> result = pairs1.join(pairs2);
		  System.out.println(result.count()); 
		  
		  Function flatten = new Function<Tuple2<String,Tuple2<String,String>>,String>(){ 
			  public String call(Tuple2<String,Tuple2<String,String>> a) { 
				  return (a._1+","+a._2._1+","+a._2._2);} };
		  
		  
		  JavaRDD<String> output = result.map(flatten);
		 
	    
	    return output;
	  }
}

