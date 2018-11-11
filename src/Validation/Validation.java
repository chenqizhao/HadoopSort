package Validation;
import java.io.IOException;
import java.util.zip.Checksum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class Validation {
	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		Path inputdir = new Path("out");
		Path outputdir = new Path("report");

		Job j=new Job(c, "Validation");

		j.setJarByClass(Validation.class);
		j.setOutputKeyClass(LongWritable.class);
		j.setOutputValueClass(Text.class);
		j.setNumReduceTasks(0);
		FileInputFormat.addInputPath(j, inputdir);
		FileOutputFormat.setOutputPath(j, outputdir);
		j.waitForCompletion(true);
		System.out.println("No error, sorting successfully.");
	    System.exit(1);
	}
	public static class MapForValidate extends Mapper<LongWritable,Text,Text,Text> {
		private LongWritable prevKey = null;
	    private String filename;

	    private String getFilename(FileSplit split) {
	      return split.getPath().getName();
	    }
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	       if (prevKey == null) {
	    	   prevKey = key;
	       } else if (prevKey.compareTo(key) > 0) {
	    	   throw new InterruptedException("sorting validation failed");
	       }
	    }
	}
}
