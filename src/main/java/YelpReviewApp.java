import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json4s.jackson.Json;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;

public class YelpReviewApp {
    public static void main(final String[] args) throws InterruptedException {
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//        SparkSession spark = SparkSession.builder()
//                .master("local[*]")
//                .appName("YelpReview")
//                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/big-data.yelp-review-2")
//                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/big-data.yelp-review-2")
//                .getOrCreate();
//
//
//        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//
//        // Load data with explicit schema
//        Dataset<YelpReview> explicitDS = MongoSpark.load(jsc).toDS(YelpReview.class);
//        explicitDS.printSchema();
//
//        // Create the temp view and execute the query
//        String[] slowWords = {"slow", "waiting", "long", "wait"};
//        String[] badFoodWords = {"gross", "bad food", "disgusting"};
//
//
//        System.out.println("START QUERY: " + java.time.LocalTime.now());
//        explicitDS.createOrReplaceTempView("reviews");
//        Dataset<Row> badReviews = spark.sql("SELECT * FROM reviews where stars < 3");
//        badReviews.count();
//
//        Dataset<Row> slowReviews = badReviews.filter((row) -> Arrays.stream(slowWords).parallel().anyMatch(row.getString(4)::contains));
//        Dataset<Row> badFoodReviews = badReviews.filter((row) -> Arrays.stream(badFoodWords).parallel().anyMatch(row.getString(4)::contains));
//
//        MongoSpark.write(slowReviews).option("collection", "yelp-slow-reviews").mode("overwrite").save();
//        MongoSpark.write(badFoodReviews).option("collection", "yelp-food-reviews").mode("overwrite").save();
//        System.out.println("END QUERY: " + java.time.LocalTime.now());
//
//
//        jsc.close();

        readJson();
    }

    public static void readJson() {
        String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

        String connectionUrl =
                "jdbc:sqlserver://localhost:1433;"
                        + "database=yelp;"
                        + "user=sa;"
                        + "password=Pepijn_seigers123;";

        try  {

            Class.forName(driver).newInstance();
            Connection connection = DriverManager.getConnection(connectionUrl);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
//
//
//        //JSON parser object to parse read file
//        JSONParser jsonParser = new JSONParser();
//
//        try (FileReader reader = new FileReader("/Volumes/Samsung_T5/datasets/yelp-dataset/yelp_academic_dataset_review.json"))
//        {
//
//            BufferedReader br = new BufferedReader(reader);
//            String line = br.readLine();
//            while(line != null) {
//            Object obj = jsonParser.parse(line);
//                JSONObject jsonObject = (JSONObject) obj;
//                System.out.println(jsonObject.get("stars"));
//            }
////            //Read JSON file
////
////            JSONArray reviewList = (JSONArray) obj;
////            System.out.println(reviewList);
//
////            //Iterate over employee array
////            reviewList.forEach( emp -> {
////
////            } );
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
    }

}
