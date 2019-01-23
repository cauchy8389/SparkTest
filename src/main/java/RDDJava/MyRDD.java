package RDDJava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.*;

/**
 * 针对RDD 使用java接口
 * Created by Administrator on 2018/8/25.
 */
public class MyRDD {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5");
        SparkConf conf  = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("wordcount");

        //驱动器
        JavaSparkContext sc = new JavaSparkContext(conf);

        //sc.textFile() 外部读取
        List<Integer> integers = Arrays.asList(1, 2, 3);
        JavaRDD<Integer> parallelize = sc.parallelize(integers);

        /*
        在java接口中通过传递匿名类形式来实现自定的函数

        Function<T, R> R call(T) 接收一个输入值并返回一个输出值，用于类似map() 和
filter() 等操作中
Function2<T1, T2, R> R call(T1, T2) 接收两个输入值并返回一个输出值，用于类似aggregate()
和fold() 等操作中
FlatMapFunction<T, R> Iterable<R> call(T) 接收一个输入
         */

        JavaRDD<Integer> map = parallelize.map(
                /*
                T1 就是RDD中 元素的类型
                R  经过call方法处理，生成类型
                public interface Function<T1, R> extends Serializable {
                        R call(T1 v1) throws Exception;
                }
                 */
                new Function<Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1) throws Exception {
                        return v1 * 2;
                    }
                }
        );

        JavaRDD<Integer> filter = parallelize.filter(
                new Function<Integer, Boolean>() {
                    @Override
                    public Boolean call(Integer v1) throws Exception {
                        return v1 % 2 == 0;
                    }
                }
        );

        JavaRDD<Integer> integerJavaRDD = parallelize.flatMap(
                //一个元素生成一个集合，这个集合中每一个元素都会成为
                //RDD中的一个元素
                //
                new FlatMapFunction<Integer, Integer>() {
                    @Override
                    public Iterator<Integer> call(Integer integer) throws Exception {
                        return Arrays.asList(1, 2, 3).iterator();
                    }
                }
        );

        integerJavaRDD.foreach(
                new VoidFunction<Integer>() {
                    @Override
                    public void call(Integer integer) throws Exception {
                        System.out.println(integer);
                    }
                }
        );


    }
}
