import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class AvgValue extends Configured implements Tool {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		private Text combo = new Text();
		private DoubleWritable avgFour = new DoubleWritable();

		public void configure(JobConf job) {
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String comboPart = "";
			String thisToken = "";
			double thisFour = 0;
			int column = 1;

			StringTokenizer tokenizer = new StringTokenizer(line, ",");
		    while (tokenizer.hasMoreTokens()) {
		    	thisToken = tokenizer.nextToken();
		    	if(column == 4)
		    		thisFour = Double.parseDouble(thisToken);
		    	
		    	else if(column >= 30 && column <= 32)
		    		comboPart = comboPart + thisToken + ",";
		    	
		    	else if(column == 33)
		    		comboPart = comboPart + thisToken;
				column = column + 1;
		    }

		    if(thisToken.equals("false")){
		    	combo.set(comboPart);
		    	avgFour.set(thisFour);
		    	output.collect(combo, avgFour);
		    }
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			double sum = 0;
			int count = 0;
			double avg = 0;
			while(values.hasNext()){
				sum = sum + values.next().get();
				count = count + 1;
			}
			avg = sum / count;
			output.collect(key, new DoubleWritable(avg));
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), AvgValue.class);
		conf.setJobName("ibm");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
    }

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AvgValue(), args);
		System.exit(res);
    }
}
