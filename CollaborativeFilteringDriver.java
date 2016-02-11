package pkg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CollaborativeFilteringDriver extends Configured implements Tool {

	@Override
	public int run(String args[]) throws Exception {
		// First Job
		Configuration conf = new Configuration();
		conf.set("similarityMethod", args[2]);
		Job job = new Job(conf, "CollaborativeFiltering1");
		job.setJarByClass(CollaborativeFilteringDriver.class);
		job.setMapperClass(MakeUserKeyMapper.class);
		job.setReducerClass(CreateIndexReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("output_intermediate"));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		// Second Job
		conf = new Configuration();
		conf.set("similarityMethod", args[2]);
		job = new Job(conf, "CollaborativeFiltering2");
		job.setJarByClass(CollaborativeFilteringDriver.class);
		job.setMapperClass(CoocurredRatingsMapper.class);
		job.setReducerClass(SimilaritiesReducer.class);

		FileInputFormat.addInputPath(job, new Path("output_intermediate"));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		return 1;
	}

	public static void main(String args[]) throws Exception {
		if (args.length < 3) {
			System.err
					.println("Please enter the arguments in the order : Input, Output, Similarity Measure");
			System.exit(-1);
		}
		if (args[2].equalsIgnoreCase("cosine")
				|| args[2].equalsIgnoreCase("pearson")) {
			int res = ToolRunner.run(new Configuration(),
					new CollaborativeFilteringDriver(), args);
			System.exit(res);
		} else {
			System.err
					.println("Please enter one of the following similarity methods: Cosine, Pearson");
		}

	}
}
