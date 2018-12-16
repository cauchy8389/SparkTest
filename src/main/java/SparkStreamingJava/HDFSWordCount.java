package SparkStreamingJava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by Administrator on 2018/9/16.
 */

//监控一个hdfs中的一个目录
public class HDFSWordCount {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5");
        SparkConf conf  = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("HDFSWordCount");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        //输入源  监控本地的目录 也可以监控hdfs目录
        //如果不写hdfs url 那么就是监控本地的目录
        JavaDStream<String> stringJavaDStream = javaStreamingContext.textFileStream(
                "hdfs://candle.hwua.com:9000/user/candle/spark/wd");

        JavaDStream<String> stringJavaDStream1 = stringJavaDStream.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(","));
                    }
                }
        );

        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream = stringJavaDStream1.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }
        );

        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream1 = stringIntegerJavaPairDStream.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );
        /*
        print 调试时候 使用，打印每个batch中的前10个元素。主要用于测试
        ，或者不需要执行output操作，用于简单触发一下 转化操作

        saveAsHadoopFiles   将每个batch中数据保存到文件中，perfix-TS.suffix

        foreachRDD 最常用的output操作，遍历DStream中的每一个产生的RDD，进行处理

         */

        stringIntegerJavaPairDStream1.print();


        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
