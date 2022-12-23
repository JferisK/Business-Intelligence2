import org.apache.spark.api.java.*;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class HelloWorldSpark {
  public static void main(String[] args) {
	
	String logFile = "/home/osboxes/data/dummy.txt"; // Should be some file on your system
    SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("a"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("b"); }
    }).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    JavaRDD<String> lines = sc.parallelize(Arrays.asList("Lines with a: " + numAs + ", lines with b: " + numBs));
    lines.saveAsTextFile("/home/osboxes/spark_output.txt");
  }
}