import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class SparkCovid19_2 {

	public static void main(String[] args) {

		if (args.length < 3) {
			System.out.println("Insufficient arguments!");
			System.exit(1);
		}
		if (args.length > 3) {
			System.out.println("Too many arguments!");
			System.exit(1);
		}

		String inputpath = args[0];
		String cacheFile = args[1];
		String outputPath = args[2];
		
		/* essential to run any spark code */
		SparkConf conf = new SparkConf().setAppName("SparkCovid19_2");
		JavaSparkContext sc = new JavaSparkContext(conf);

		/* load input data to RDD */
		JavaRDD<String> dataRDD = sc.textFile(inputpath);
		
		Map<String, Long> populationMap = new HashMap<>();
		Scanner scanner;
		try {
			scanner = new Scanner(new File(cacheFile));
			String popRecord = scanner.nextLine();
//			Broadcast<HashMap<String, String>> br = sc.sparkContext().broadcast(new HashMap<>());
			while (scanner.hasNextLine()) {
				popRecord = scanner.nextLine();
				String popRecString = popRecord.toString();
				String popArray[] = popRecString.split(",");
				// [country],[location],[continent],[population_year],[population]
				if (popArray.length == 5) {
					String locationFromPopulationCSV = popArray[1];
					Long populationVal = Long.parseLong(popArray[4]);
					populationMap.put(locationFromPopulationCSV, populationVal);
				}
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		
		Broadcast<Map<String, Long>> population = sc.broadcast(populationMap);

		JavaPairRDD<String, Double> counts = dataRDD.flatMapToPair(line -> {
			List<Tuple2<String, Double>> retWords = new ArrayList<Tuple2<String, Double>>();
			try {
				if (!line.isEmpty() && !line.contains("location")) {
					String[] inputRecords = line.split(",");
					String locationFromCovidCSV = inputRecords[1];
					double newCasesCount = Double.parseDouble(inputRecords[2]);
					Long population_count = population.value().get(locationFromCovidCSV);
					//Long population_count = populationMap.get(locationFromCovidCSV);
					if (population_count != null) {
						double casesPerMillion = newCasesCount / population_count * 1000000.0;
						retWords.add(new Tuple2<String, Double>(locationFromCovidCSV, casesPerMillion));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			return retWords.iterator();

		}).reduceByKey((x, y) -> x + y);

		counts.saveAsTextFile(outputPath);

	}

}
