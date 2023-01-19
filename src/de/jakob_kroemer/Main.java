package de.jakob_kroemer;
import org.apache.spark.api.java.*;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Main {
	public static void main(String[] args) {
		
		String user = "Noah";
		
		String file1 = "";
		String file2 = "";
		
		switch(user){
			case "Noah":
				file1 = "/home/osboxes/data/Aufgabe2/trip_data/trip_data_1.csv"; // Should be some file on your system
				file2 = "/home/osboxes/data/Aufgabe2/trip_fare/trip_fare_1.csv";
				break;
			case "Malte":
				file1 = "/home/osboxes/data/Aufgabe2/trip_data/trip_data_1.csv"; // Should be some file on your system
				file2 = "/home/osboxes/data/Aufgabe2/trip_fare/trip_fare_1.csv";
				break;
			case "Daniel":
				file1 = "/home/osboxes/data/trip_data_1.csv"; // Should be some file on your system
				file2 = "/home/osboxes/data/trip_fare_1.csv";
				break;
			case "Jakob":
				file1 = "/home/osboxes/data/trip_data_1.csv";
				file2 = "/home/osboxes/data/trip_fare_1.csv";
				break;
		}
		
		JavaRDD output = Merge.merge(file1,file2);
		
		Q2 Q2 = new Q2(output);
		Q2.calcResult();
		
		Q6 Q6 = new Q6(output);
		Q6.calcResult();
		
		Q8 Q8 = new Q8(output);
		Q8.calcResult();
		
		Q7 Q7 = new Q7(output);
		Q7.calcResult();
		
		A_Noah Q9 = new A_Noah(output);
		Q9.calcResult();
		
		Q4 Q4 = new Q4(output);
		Q4.calcResult();
		
	}
}
