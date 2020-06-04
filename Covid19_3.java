import java.io.IOException;
import java.text.ParseException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;

public class Covid19_3 {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable>{

        private static DoubleWritable mappingVal = new DoubleWritable(1);
        private Text location = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

          String input = value.toString(); 
          if (input.contains("date"))                                                                                      
            return; 
          // String header = "date,location,new_cases,new_deaths";
          String[] inputRow = input.split(",");
          
          location.set(inputRow[1]);
          double newCasesCount = Double.parseDouble(inputRow[2]);
          mappingVal.set(newCasesCount);
          context.write(location, mappingVal);
        }
  }

  public static class IntSumReducer extends Reducer<Text, DoubleWritable, String, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();  
        private Map<Text, Long> populationMap = new HashMap<>();
        private Text location = new Text();
        // private DoubleWritable population = new DoubleWritable();

        //[country],[location],[continent],[population_year],[population]
        public void setup(Context context){
          try {
            URI[] files = context.getCacheFiles();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            for (URI file : files){
              FSDataInputStream fileReader = fs.open(new Path(file.getPath()));
              Scanner scanner = new Scanner(fileReader);
              String popRecord = scanner.nextLine();
              while(scanner.hasNextLine()) {
                popRecord = scanner.nextLine();
                String popRecString = popRecord.toString();
                String popArray[] = popRecString.split(",");
                if(popArray.length == 5 || (!popArray[4].isEmpty())){
                  location.set(popArray[1]);
                  Long populationVal = Long.parseLong(popArray[4]);
                  // population.set(populationVal);
                  populationMap.put(location, populationVal);
                }
              }
              fileReader.close();
            }
          }catch( IOException e) {
            e.printStackTrace();
          }
        }

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

          double totalcases = 0;  
          String keyValue = key.toString();
          for (DoubleWritable val: values) {                                                                                                     
            totalcases += val.get();                                                                                                                
          } 
          Long count = populationMap.get(keyValue);
          if(count != null) { 
            double casesPerMillion = totalcases/count * 1000000.0;
            result.set(casesPerMillion);                                                                                                                      
            context.write(keyValue, result);
          } 
        }
  }

  public static void main(String[] args) throws Exception {
    
    if (args.length < 3) {
      System.out.println("Insufficient arguments!");
      System.exit(1);
    }
    if (args.length > 3) {
      System.out.println("Too many arguments!");
      System.exit(1);
    }
    
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "covid19_3 count");
    job.setJarByClass(Covid19_3.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);                                                                                                   
    job.setOutputKeyClass(String.class);  
    job.setOutputValueClass(DoubleWritable.class);

    FileSystem hdfs = FileSystem.get(conf);
    String inputPath = args[0];
    if (hdfs.exists(new Path(inputPath))) {
      FileInputFormat.addInputPath(job, new Path(inputPath));
    } else {
      System.out.println(inputPath + " does not exist.");
      System.exit(1);
    }
    String cacheFile = args[1];
    if(hdfs.exists(new Path(cacheFile))) {
      Path hadoopCacheFilePath = new Path("hdfs://" + hdfs.getCanonicalServiceName() + "/" + cacheFile);
      job.addCacheFile(hadoopCacheFilePath.toUri()); 
    } else {
      System.out.println(cacheFile + " does not exist.");
      System.exit(1);
    }
      
    //Uncommenting this would set the number of reduce tasks                                                                                             
    //job.setNumReduceTasks(2);                  

    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    if (hdfs.exists(new Path(args[2])))
        hdfs.delete(new Path(args[2]), true);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}