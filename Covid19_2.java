import java.io.IOException;
import java.util.StringTokenizer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;

public class Covid19_2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, LongWritable>{

        // private final static IntWritable one = new IntWritable(1);
        private static LongWritable mappingVal = new LongWritable(1);
        private Text word = new Text();
        private String startingDateString = null, endingDateString = null;

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {

          String input = value.toString(); 
          // String header = "date,location,new_cases,new_deaths";
          // while(input != header) {
          long deathcases = 0L;
          String[] inputRow = input.split(",");
          
          SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
          Date currentDate = null, startDate = null, endDate = null;

          Configuration conf = new Configuration();
          conf = context.getConfiguration();
          if(conf != null) {
            startingDateString = conf.get("start_date");
            endingDateString = conf.get("end_date");
          }

          try {
            currentDate = simpleDateFormat.parse(inputRow[0]);
            startDate = simpleDateFormat.parse(startingDateString);
            endDate = simpleDateFormat.parse(endingDateString);
          
            if(!(currentDate.before(startDate) || currentDate.after(endDate))) {  
              word.set(inputRow[1]);
              deathcases = Long.parseLong(inputRow[3]);
              mappingVal.set(deathcases);
              context.write(word, mappingVal);
            }
          } catch (ParseException e) {
            e.printStackTrace();
          }
        }
  }

  public static class IntSumReducer
       extends Reducer<Text,LongWritable,Text,LongWritable> {
    private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

      int sum = 0;
      for (LongWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    if (args.length < 4) {
      System.out.println("Insufficient arguments!");
      System.exit(1);
    }
    if (args.length > 4) {
      System.out.println("Too many arguments!");
      System.exit(1);
    }

    conf.set("start_date", args[1]);
    conf.set("end_date", args[2]);
    
    Job job = Job.getInstance(conf, "covid19_2 count");
    job.setJarByClass(Covid19_2.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    // FileInputFormat.addInputPath(job, new Path(args[0]));
    FileSystem hdfs = FileSystem.get(conf);
    String inputPath = args[0];
    if (hdfs.exists(new Path(inputPath))) {
      FileInputFormat.addInputPath(job, new Path(inputPath));
    } else {
      System.out.println(inputPath + " doesn't exist.");
      System.exit(1);
    }

    FileOutputFormat.setOutputPath(job, new Path(args[3]));

    if (hdfs.exists(new Path(args[3])))
        hdfs.delete(new Path(args[3]), true);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}