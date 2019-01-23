package sparkproject;

import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.SparkSession;
import sparkproject.sessionAnalysis.CategorySortKey;

/**
 * Spark工具类
 * @author Administrator
 *
 */
public class SparkUtils {

	public static SparkConf getSparkConf(){
		return new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_SESSION)
//				.set("spark.default.parallelism", "100")
				.set("spark.storage.memoryFraction", "0.5")
				.set("spark.shuffle.consolidateFiles", "true")
				.set("spark.shuffle.file.buffer", "64")
				.set("spark.shuffle.memoryFraction", "0.3")
				.set("spark.reducer.maxSizeInFlight", "24")
				.set("spark.shuffle.io.maxRetries", "60")
				.set("spark.shuffle.io.retryWait", "60")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		        //.set("spark.driver.allowMultipleContexts","true")
				.registerKryoClasses(new Class[]{
						CategorySortKey.class,
						IntList.class});
		/*
		 * 比如，获取top10热门品类功能中，二次排序，自定义了一个Key
		 * 那个key是需要在进行shuffle的时候，进行网络传输的，因此也是要求实现序列化的
		 * 启用Kryo机制以后，就会用Kryo去序列化和反序列化CategorySortKey
		 * 所以这里要求，为了获取最佳性能，注册一下我们自定义的类
		 */
	}

	/**
	 * 根据当前是否本地测试的配置
	 * 决定，如何设置SparkConf的master
	 */
	public static void setMaster(SparkConf conf) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			conf.setMaster("local");  
		}  
	}
	
	/**
	 * 获取SQLContext
	 * 如果spark.local设置为true，那么就创建SQLContext；否则，创建HiveContext
	 * @param ss
	 * @return
	 */
	public static SQLContext getSQLContext(SparkSession ss) {
		//SparkSession

//		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
//		if(local) {
		return new SQLContext(ss);
//		} else {
//			return new HiveContext(sc);
//		}
	}
	
	/**
	 * 生成模拟数据
	 * 如果spark.local配置设置为true，则生成模拟数据；否则不生成
	 * @param jsc
	 * @param sqlContext
	 */
	public static void mockData(JavaSparkContext jsc, SQLContext sqlContext) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			MockData.mock(jsc, sqlContext);
		}
	}
	
	/**
	 * 获取指定日期范围内的用户行为数据RDD
	 * @param sqlContext
	 * @param taskParam
	 * @return
	 */
	public static JavaRDD<Row> getActionRDDByDateRange(
			SQLContext sqlContext, JSONObject taskParam) {
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		
		String sql = 
				"select * "
				+ "from user_visit_action "
				+ "where date>='" + startDate + "' "
				+ "and date<='" + endDate + "'";  
//				+ "and session_id not in('','','')"

		Dataset<Row> actionDF = sqlContext.sql(sql);
		
		/**
		 * 这里就很有可能发生上面说的问题
		 * 比如说，Spark SQl默认就给第一个stage设置了20个task，但是根据你的数据量以及算法的复杂度
		 * 实际上，你需要1000个task去并行执行
		 * 
		 * 所以说，在这里，就可以对Spark SQL刚刚查询出来的RDD执行repartition重分区操作
		 */
		
//		return actionDF.javaRDD().repartition(1000);
		
		return actionDF.javaRDD();
	}
	
}
