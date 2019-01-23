package SparkStreamingJava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;


/**
 * Created by Administrator on 2018/9/16.
 */
public class WindowsWordCountJava {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5");
        SparkConf conf  = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("WindowsWordCountJava");
        JavaStreamingContext javaStreamingContext =
                //batch时间
                new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> stringJavaReceiverInputDStream = javaStreamingContext.
                socketTextStream("candle.hwua.com", 9999);

        JavaDStream<String> stringJavaDStream = stringJavaReceiverInputDStream.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" ")).iterator();
                    }
                }
        );

        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream = stringJavaDStream.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }
        );

        /**
         * 该算子 一共三个参数
         * 第一个参数是 处理的逻辑
         * 第二个参数是设置窗口的长度
         * 第三个参数设置的是滑动的步长
         */
        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream1 = stringIntegerJavaPairDStream.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }, Durations.seconds(60), Durations.seconds(10)
        );

        stringIntegerJavaPairDStream1.print();

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
