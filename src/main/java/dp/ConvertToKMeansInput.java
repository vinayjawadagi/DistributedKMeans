package dp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ConvertToKMeansInput extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(ConvertToKMeansInput.class);

    public static class JoinMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            // Split Key(Zip code) and value (population or business data)
            String[] data = value.toString().split("-joindelimiter-");

            // Emit (Zip code, population) or (Zip code, business data)
            context.write(new Text(data[0]), new Text(data[1]));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, NullWritable> {

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context)
                throws IOException, InterruptedException {
            List<String> businesses = new ArrayList<>(); // List of all the businesses in the zip code
            String population = ""; // Only one population count per zip code
            String zipCode = key.toString();

            // Look for businesses data and population
            for (var data : values) {
                if (data.toString().split("-delimiter-").length == 1) {
                    population = data.toString();
                } else {
                    businesses.add(data.toString());
                }
            }
            // If businesses data and population data both exist for this Zip code then emit
            // each business data with population count
            if (!businesses.isEmpty() && !population.isEmpty()) {
                for (var business : businesses) {
                    context.write(new Text(zipCode + "-delimiter-" + business + "-delimiter-" + population),
                            NullWritable.get());
                }
            }
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(ConvertToKMeansInput.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0] + "/tokmeans"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // runs the job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new ConvertToKMeansInput(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}