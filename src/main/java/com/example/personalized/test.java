package com.example.personalized;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class test {
    public static void main(String[] args) {
        SparkSession sprak = SparkSession.builder().appName("personalized").master("local[*]").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sprak.sparkContext());

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        numbersRDD.foreach(n -> System.out.println(n));
    }
}
