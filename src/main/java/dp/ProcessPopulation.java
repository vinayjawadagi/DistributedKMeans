package dp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ProcessPopulation extends Configured implements Tool {
    public static String getDataText(String input) {
        String[] strs = input.split(" "); // Tokenize the input

        String zipCode = strs[2]; // Extract zip code
        String population = strs[3]; // Extract population

        return zipCode + "-joindelimiter-" + population;
    }

    private static final Logger logger = LogManager.getLogger(ProcessPopulation.class);

    public static class PopulationMapper extends Mapper<Object, Text, Text, NullWritable> {

        @Override
        public void map(final Object key, final Text input, final Context context)
                throws IOException, InterruptedException {
            // Remove quotes and replace commas with space from the input
            String inputString = input.toString().replaceAll("\"", "").replaceAll(",", " ");

            // Filter out the required fields i.e. zip code and population
            String populationData = getDataText(inputString);

            context.write(new Text(populationData), NullWritable.get());
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Pop Data");
        job.setJarByClass(ProcessPopulation.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        job.setMapperClass(PopulationMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0] + "/population"));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/population"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new ProcessPopulation(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}