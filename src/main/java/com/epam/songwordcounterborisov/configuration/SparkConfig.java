package com.epam.songwordcounterborisov.configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;
import org.apache.tomcat.JarScannerCallback;
import org.springframework.context.annotation.*;
import org.springframework.test.context.jdbc.SqlConfig;

import java.util.List;

@Configuration
public class SparkConfig {
    public static final String PROD ="PROD";
    public static final String DEV ="DEV";

    @Bean
    public SQLContext sqlContext(SparkContext sc){
        return new SQLContext(sc);
    }

    @Bean
    public Broadcast<List<String>> garbage(JavaSparkContext sc){
        List<String> list = List.of("in", "a", "i", "this", "that", "you");
        Broadcast<List<String>> listBroadcast = sc.broadcast(list);
        return listBroadcast;
    }
    @Bean
    public JavaSparkContext javaSparkContext(SparkContext sc){
     return new JavaSparkContext(sc);
    }
    @Bean
    public SparkContext sc(SparkConf conf){
        return new SparkContext(conf);
    }

    @Profile("PROD")
    @Bean
    public SparkConf sparkConfProd(){
    return new SparkConf().setAppName("words").setMaster("yarn[*]");
    }

    @Profile("DEV")
    @Primary
    @Bean
    public SparkConf sparkConfDev(){
    return new SparkConf().setAppName("words").setMaster("local[*]");
    }
}
