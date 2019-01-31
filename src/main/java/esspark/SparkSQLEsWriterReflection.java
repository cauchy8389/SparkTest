package esspark;

import mybean.Crime;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by vishalshukla on 22/08/15.
 */
public class SparkSQLEsWriterReflection {
    public static void main(String args[]) {
        System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5");

        SparkConf conf = new SparkConf().setAppName("esh-spark").setMaster("local[4]");
        conf.set("es.index.auto.create", "true");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> textFile = context.textFile("hdfs://zhy.cauchy8389.com:9000/user/zhy/eshadoop/crimes_dataset.csv");

        JavaRDD<Crime> dataSplits = textFile.map(SparkEsWriter.funcMap);

        SQLContext sqlContext = new SQLContext(context);
        Dataset<Row> df = sqlContext.createDataFrame(dataSplits, Crime.class);

        df.registerTempTable("crime");

        Dataset<Crime> ds = df.as(Encoders.kryo(Crime.class));
        JavaEsSparkSQL.saveToEs(ds, "esh_sparksql/crimes_reflection");
    }

}
