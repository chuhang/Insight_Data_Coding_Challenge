/*
File Name: Median.java
by: Hang Chu, Cornell University
Insight Data Engineer Coding Challenge

Use Hadoop MapReduce to count number of words per line in a scalable way
*/
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Median 
{

  /*
  Mapper: Produce one <key,value> pair for each line
  where key = byteoffset  filename
  value = number of words in that line
  */
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
  {
  	private IntWritable result = new IntWritable();
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
      StringTokenizer itr = new StringTokenizer(value.toString());
      int sum=0;
      while (itr.hasMoreTokens()) 
      {
        word.set(itr.nextToken());
        sum++;
      }
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String filename = fileSplit.getPath().getName();
      result.set(sum);
      context.write(new Text(key.toString() + '\t' + filename), result);
    }
  }

  /*
  Reducer: Moving median is not easily serilizable,
  thus do nothing but copy what the Mapper has produced.
  */
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
  {
    private IntWritable result = new IntWritable();
    private final static IntWritable one = new IntWritable(1);

    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
    {
      int sum = 0;
      for (IntWritable val : values) 
      {
        sum = val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  /*
  The main function: Set up a Hadoop MapReduce job, wait it to complete. 
  */
  public static void main(String[] args) throws Exception 
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Median.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
