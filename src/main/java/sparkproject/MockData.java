package sparkproject;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;



/**
 * 模拟数据程序
 * @author Administrator
 *
 */
public class MockData {

    /**
     * 弄你数据
     *
     * @param sc
     * @param sqlContext
     */
    public static void mock(JavaSparkContext sc,
                            SQLContext sqlContext) {
        List<Row> rows = new ArrayList<Row>();

        String[] searchKeywords = new String[]{"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
                "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};

        LocalDate now = LocalDate.now();
        String date = now.toString();
        String[] actions = new String[]{"search", "click", "order", "pay"};
        Random random = new Random();

        for (int i = 0; i < 100; i++) {
            long userid = random.nextInt(100);

            for (int j = 0; j < 10; j++) {
                String sessionid = UUID.randomUUID().toString().replace("-", "");
                String baseActionTime = date + " " + StringUtils.leftPad(String.valueOf(random.nextInt(23)),2, '0');

                for (int k = 0; k < random.nextInt(100); k++) {
                    long pageid = random.nextInt(10);
                    String actionTime = baseActionTime + ":" + StringUtils.leftPad(String.valueOf(random.nextInt(59)),2,'0')
                            + ":" + StringUtils.leftPad(String.valueOf(random.nextInt(59)),2,'0');
                    String searchKeyword = null;
                    Long clickCategoryId = null;
                    Long clickProductId = null;
                    String orderCategoryIds = null;
                    String orderProductIds = null;
                    String payCategoryIds = null;
                    String payProductIds = null;

                    String action = actions[random.nextInt(4)];
                    if ("search".equals(action)) {
                        searchKeyword = searchKeywords[random.nextInt(10)];
                    } else if ("click".equals(action)) {
                        clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
                        clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));
                    } else if ("order".equals(action)) {
                        orderCategoryIds = String.valueOf(random.nextInt(100));
                        orderProductIds = String.valueOf(random.nextInt(100));
                    } else if ("pay".equals(action)) {
                        payCategoryIds = String.valueOf(random.nextInt(100));
                        payProductIds = String.valueOf(random.nextInt(100));
                    }

                    Row row = RowFactory.create(date, userid, sessionid,
                            pageid, actionTime, searchKeyword,
                            clickCategoryId, clickProductId,
                            orderCategoryIds, orderProductIds,
                            payCategoryIds, payProductIds);
                    rows.add(row);
                }
            }
        }

        JavaRDD<Row> rowsRDD = sc.parallelize(rows);

        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("session_id", DataTypes.StringType, true),
                DataTypes.createStructField("page_id", DataTypes.LongType, true),
                DataTypes.createStructField("action_time", DataTypes.StringType, true),
                DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
                DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
                DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
                DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true)));

        DataFrame df = sqlContext.createDataFrame(rowsRDD, schema);

        df.registerTempTable("user_visit_action");
        for (Row _row : df.take(10)) {
            System.out.println(_row);
        }

        /**
         * ==================================================================
         */

        rows.clear();
        String[] sexes = new String[]{"male", "female"};
        for (int i = 0; i < 100; i++) {
            long userid = i;
            String username = "user" + i;
            String name = "name" + i;
            int age = random.nextInt(60);
            String professional = "professional" + random.nextInt(100);
            String city = "city" + random.nextInt(100);
            String sex = sexes[random.nextInt(2)];

            Row row = RowFactory.create(userid, username, name, age,
                    professional, city, sex);
            rows.add(row);
        }

        rowsRDD = sc.parallelize(rows);

        StructType schema2 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("professional", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true)));

        DataFrame df2 = sqlContext.createDataFrame(rowsRDD, schema2);
        for (Row _row : df2.take(2)) {
            System.out.println(_row);
        }

        df2.registerTempTable("user_info");
    }

    public static void main(String[] args) {
        System.out.println(StringUtils.leftPad("adfff",2, '0'));
        Random random = new Random();
        System.out.println(StringUtils.leftPad(String.valueOf(random.nextInt(59)),2,'0'));

        LocalDateTime curDate = LocalDateTime.now();

        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        LocalDateTime preDate = curDate.plusDays(-1);
        LocalDateTime nextDate = curDate.plus(1, ChronoUnit.DAYS);

        DateTimeFormatter formatter = ParamUtils.FORMATTER_DATETIMES;
        //formatter.
        System.out.println(preDate.format(formatter));
        System.out.println(curDate.format(DateTimeFormatter.ISO_DATE));
        System.out.println(nextDate.format(DateTimeFormatter.ISO_TIME));

        System.out.println(curDate.isBefore(preDate));
        System.out.println(curDate.isBefore(nextDate));

        //Duration.
        Duration duration = Duration.between(curDate,LocalDateTime.now());
        System.out.println(duration.toMillis());


        //Date LocalDateTime互相转换
        //DateTime adt = new DateTime()
        LocalDate holiday = LocalDate.parse("2018-12-25", DateTimeFormatter.ISO_DATE);
        ZoneId zoneId = ZoneId.systemDefault();

        LocalDateTime ldtHoliday = holiday.atStartOfDay();
        ZonedDateTime zdt = ldtHoliday.atZone(zoneId);
        Date date = Date.from(zdt.toInstant());

        Instant instant = date.toInstant();
        LocalDateTime localDateTime = instant.atZone(zoneId).toLocalDateTime();
        System.out.println(localDateTime.format(DateTimeFormatter.ISO_DATE));

        //zhy comment
    }

}