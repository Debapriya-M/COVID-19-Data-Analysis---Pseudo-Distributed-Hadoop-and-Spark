import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class SparkCovid19_1 {

    public static void main(String[] args) {


        if (args.length < 4) {
            System.out.println("Insufficient arguments!");
            System.exit(1);
        }
        if (args.length > 4) {
            System.out.println("Too many arguments!");
            System.exit(1);
        }

        String inputpath = args[0];
        String beginningDate = args[1];
        String endingDate = args[2];
        String output = args[3];

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        /* essential to run any spark code */
        SparkConf conf = new SparkConf().setAppName("SparkCovid19_1");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /* load input data to RDD */
        JavaRDD<String> dataRDD = sc.textFile(inputpath);

        JavaPairRDD<String, Long> counts = dataRDD
                .flatMapToPair(line -> {
                    List<Tuple2<String, Long>> retWords = new ArrayList<Tuple2<String, Long>>();
                    try {

                        if (!line.isEmpty() && !line.contains("location")) {
                            String[] inputRecords = line.split(",");
                            long deathcases = 0L;
                            Date currentDate = null, startDate = null, endDate = null;
                            startDate = simpleDateFormat.parse(beginningDate);
                            endDate = simpleDateFormat.parse(endingDate);
                            String location = inputRecords[1];
                            deathcases = Long.parseLong(inputRecords[3]);
                            currentDate = simpleDateFormat.parse(inputRecords[0]);
                            if (currentDate.compareTo(startDate) >= 0 && currentDate.compareTo(endDate) <= 0) {
                                retWords.add(new Tuple2<String, Long>(location, deathcases));
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return retWords.iterator();
                })
                .reduceByKey((x, y) -> x + y);


        counts.saveAsTextFile(output);
    }
}