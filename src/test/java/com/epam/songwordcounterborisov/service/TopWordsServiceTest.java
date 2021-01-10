package com.epam.songwordcounterborisov.service;

import com.epam.songwordcounterborisov.configuration.MainConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.epam.songwordcounterborisov.configuration.SparkConfig.DEV;
import static org.junit.Assert.*;
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = MainConf.class)
@ActiveProfiles(DEV)
public class TopWordsServiceTest {

    @Autowired
    private SQLContext sqlContext;

    @Autowired
    private TopWordsService topWordsService;
    
    @Autowired
    private JavaSparkContext javaSparkContext;


    @Test
    public void sql(){
        SparkSession session = SparkSession.builder().appName("hi").master("local[*]").getOrCreate();
        Dataset<Row> dataset = session.read().json("src/main/songs/names.json");
        Dataset<Row> dataFrame = sqlContext.read().json("src/main/songs/names.json");
        Dataset<Row> salaryCount = dataset.withColumn("salary", functions.col("age").multiply(10).multiply(functions.size(functions.col("keywords"))));
        salaryCount.show();
        Column column = functions.explode(functions.col("keywords"));
        Dataset<Row> technology = salaryCount.withColumn("technology", column);
        Dataset<Row> countTechnology = technology.groupBy("technology").count();
        countTechnology.show();
        Dataset<Row>popular = countTechnology.sort(functions.col("count").desc()).filter(functions.col("count").geq(2));
        Dataset<Row> rowDataset = technology.filter(functions.col("salary").leq(1200));
        rowDataset.filter(functions.col("technology").equalTo(popular.col("technology"))).show();
        Stream.of(dataFrame.dtypes()).forEach(System.err::println);

   }

    @Test
    public void topX() {
        JavaRDD<String> rdd = javaSparkContext.parallelize(List.of("java","in",
                "a","a","a","a","scala","scala"));
        scala.collection.immutable.List<String> list = topWordsService.topX(rdd.rdd(), 1);
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals("scala", list.iterator().next());
    }
}