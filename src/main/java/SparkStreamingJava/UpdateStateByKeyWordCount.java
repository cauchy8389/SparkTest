package SparkStreamingJava;

import com.google.common.base.Optional;
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

import java.util.Arrays;
import java.util.List;

/**
 * Created by Administrator on 2018/9/16.
 */
//做一次 全局的单词统计
public class UpdateStateByKeyWordCount {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5");
        SparkConf conf  = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("UpdateStateByKeyWordCount");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        /*
        注意事项：
        1.如果要使用updateStateByKey 算子，必须要设置一个checkpoint目录，开启
        checkpoint机制

        这样的话 才能把每个key对应的state 除了在内存中保存，那么也在checkpoint保存一份
        因为 spark streaming 需要长期保存一份数据，那么spark streaming
        是要求必须开启checkpoint，以便在内存数据丢失的时候，可以从
        checkpoint中恢复数据
         */
        javaStreamingContext.checkpoint(
                 "hdfs://candle.hwua.com:9000/user/candle/spark/wd_checkpoint");

        JavaReceiverInputDStream<String> stringJavaReceiverInputDStream = javaStreamingContext.
                   socketTextStream("candle.hwua.com", 9999);


        JavaDStream<String> stringJavaDStream = stringJavaReceiverInputDStream.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" "));
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
         * 如果调用reduceByKey  那么 只能在单一的一个批次做统计
         *
         * 如果是全局统计  从进程开始执行，到目前为止，每个单词出现的次数
         * 使用updateStateByKey
         *
         */
       //
        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream1 = stringIntegerJavaPairDStream.updateStateByKey(
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    @Override
                    public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
                        //v1  针对每一个key  生成一个集合
                        //v2  是上一次聚集的集合中对应的key的value  Optional
                        //可能存在，也可能不存在
                        //首先定义一个全局的单词计数

                        Integer newValue = 0;

                        //判断 v2是否存在，如果不存在，意味着这个key是第一次出现，只要把v1聚集即可
                        //如果v2存在，需要把v1聚合之后还要和v2聚合

                        if (v2.isPresent()) {
                            newValue = v2.get();
                        }

                        for (Integer value : v1) {
                            newValue += value;
                        }

                        return Optional.of(newValue);
                    }
                }

        );
        //到这里位置，每个batch，都会执行全局的统计
        //打印出来
        stringIntegerJavaPairDStream1.print();

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

        //注意事项: 使用updateStateByKey  算子要注意  数据量，sparkStreaming流式计算
        //会一直运算，防止updateStateByKey导致 ，集合过于庞大。
        //可以把每个batch中的数据 插入到 mysql中
    }
}
