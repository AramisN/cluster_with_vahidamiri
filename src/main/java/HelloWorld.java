import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class HelloWorld {

    public static void main(String[] args) {

        String inputPath = args[0];
        SparkConf conf = new SparkConf().setAppName("word-counter");//.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile(inputPath);
        JavaPairRDD<String, Integer> counts = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Integer>> finalCounts = counts.collect();

        for(Tuple2<String, Integer> count: finalCounts)
            System.out.println(count);
    }
}
