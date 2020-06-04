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

public class Covid19_1 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, LongWritable>{

        // private final static IntWritable one = new IntWritable(1);
        private static LongWritable mappingVal = new LongWritable(1);
        private Text word = new Text();
        private String flag = "false";

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {

          String input = value.toString(); 
          // String header = "date,location,new_cases,new_deaths";
          // while(input != header) {
          long cases = 0L;
          String[] inputRow = input.split(",");
          String ref = "2019-12-31";
          SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-DD");
          Date currentDate = null, referenceDate = null;

          Configuration conf = new Configuration();
          conf = context.getConfiguration();
          if(conf != null) {
            flag = conf.get("flag");
          }

          try {
            currentDate = simpleDateFormat.parse(inputRow[0]);
            referenceDate = simpleDateFormat.parse(ref);
          
            if(currentDate.after(referenceDate)) {  
              if(!(!(flag == "true") && (inputRow[1].equals("World")))) {
                word.set(inputRow[1]);
                cases = Long.parseLong(inputRow[2]);
                mappingVal.set(cases);
                context.write(word, mappingVal);
              }
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

    if (args.length < 3) {
      System.out.println("Insufficient arguments!");
      System.exit(1);
    }
    if (args.length > 3) {
      System.out.println("Too many arguments!");
      System.exit(1);
    }

    conf.set("flag", args[1]);
    
    Job job = Job.getInstance(conf, "covid19_1 count");
    job.setJarByClass(Covid19_1.class);
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

    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    if (hdfs.exists(new Path(args[2])))
        hdfs.delete(new Path(args[2]), true);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}