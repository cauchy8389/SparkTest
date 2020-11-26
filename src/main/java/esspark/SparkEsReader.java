package esspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;
/**
 * Created by zhy on 2020/11/26.
 */
public class SparkEsReader {

    public static void main(String args[]){
        SparkConf conf = new SparkConf().setAppName("esh-spark").setMaster("local[4]");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<Map<String, Object>> esRDD =
                JavaEsSpark.esRDD(context, "esh_spark/crimes", "{\"query\" : { \"term\" : { \"primaryType\" : \"theft\" } } }").values();

        for(Map<String,Object> item: esRDD.collect()){
            System.out.println(item);
        }

    }
}
