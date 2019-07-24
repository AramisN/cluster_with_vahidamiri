import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class HelloWorld {

    public static void main(String[] args) {


        System.out.println("Hello World!");

        System.out.println("Hello");
//        System.out.println(System.getProperty("hadoop.home.dir"));

        String inputPath = args[0];
//        String outputPath = args[1];
//        String inputPath = "C:\\Users\\amirh\\Desktop\\New Text Document (5).txt";

        SparkConf conf = new SparkConf().setAppName("word-counter").setMaster("local");//.set("spark.cores.max", "10");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile(inputPath);
        JavaPairRDD<String, Integer> counts = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Integer>> finalCounts = counts//.filter((x) -> x._1().contains("@"))
                .collect();

        for(Tuple2<String, Integer> count: finalCounts)
            System.out.println(count);
    }
}
