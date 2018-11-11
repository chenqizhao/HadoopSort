package Sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class Sort {
	public static class MapForSort extends Mapper<LongWritable, Text, Text, Text> {
	    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
	        con.write(value, new Text());
	    }
	}
	public static class ReduceForSort extends Reducer<Text, Text, Text, Text> {
	    public void reduce(Text key, Text values, Context con) throws IOException, InterruptedException {
	        con.write(key, values);
	    }
	}
	public static class KVPair implements Writable, WritableComparable<KVPair> {
		private String key;
		private String value;
		public KVPair() {
		}
		public KVPair(String key, String value) {
			this.key = key;
			this.value = value;
		}
		public void setKey(String key) {
			this.key = key;
		}
		public String getKey() {
			return this.key;
		}
		public void setValue(String value) {
			this.value = value;
		}
		public String getValue() {
			return this.value;
		}
		public int compareTo(KVPair p) {
			int result = this.key.compareTo(p.getKey());
			if (result == 0) {
				result = value.compareTo(p.getValue());
			}
			return result;
		}
		@Override
		public String toString() {
			return this.key.toString() + ",";
		}
		@Override
		public void readFields(DataInput dataInput) throws IOException {
			this.key = WritableUtils.readString(dataInput);
			this.value = WritableUtils.readString(dataInput);
		}
		@Override
		public void write(DataOutput dataOutput) throws IOException {
			// TODO Auto-generated method stub
			WritableUtils.writeString(dataOutput, this.key);
			WritableUtils.writeString(dataOutput, this.value);
		}
		public int getPartitionerCode() {
			return Integer.valueOf(this.key.charAt(0));
		}
	}
	public static class KVPartitioner extends Partitioner<Text, Text> {
		public int getPartition(Text key, Text value, int reducers) {
			return Math.abs(key.toString().charAt(0) * key.toString().charAt(1)) % reducers; 
		}
	}
	public static class KVGroupComparator extends WritableComparator {
		public KVGroupComparator() {
			super(KVPair.class, true);
		}
		@Override 
		public int compare(WritableComparable a, WritableComparable b) {
			KVPair ka = (KVPair) a;
			KVPair kb = (KVPair) b;
			return ka.compareTo(kb);
		}
	}

	public static void main(String [] args) throws Exception {
		Configuration c = new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path inputdir = new Path("GenText/part-00000");
		Path outputdir = new Path("out");
		Job j = new Job(c, "Sort");
		j.setJarByClass(Sort.class);
		j.setMapperClass(MapForSort.class);
		j.setReducerClass(ReduceForSort.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(Text.class);
		//j.setPartitionerClass(KVPartitioner.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		//j.setGroupingComparatorClass(KVGroupComparator.class);
		j.setNumReduceTasks(10);
		FileInputFormat.addInputPath(j, inputdir);
		FileOutputFormat.setOutputPath(j, outputdir);
		j.waitForCompletion(true);
	}
}
