import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;

public class ShowNumbers {

	public static boolean isNumeric(String str) {
		return str.matches("-?\\d+(\\.\\d+)?");
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				if (isNumeric(word.toString())) {
					context.write(word, value);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String values_ = "";
			for (Text val : values) {
				values_ += val.toString();
			}
			result.set(values_);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.out.println("ShowNumbers <nameFile> <outDir>");
			ToolRunner.printGenericCommandUsage(System.out);
			return;
		}
		Job job = Job.getInstance();
		job.setJarByClass(ShowNumbers.class);
		job.setJobName("ShowNumbers");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}