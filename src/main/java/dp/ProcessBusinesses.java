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

import dp.classes.Business;

public class ProcessBusinesses extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(ProcessBusinesses.class);

    public static String getDataText(Text input) {
        // Parse input json object to datatype Business
        Business business = new Business(input.toString());
        // Return business string in required format
        return business.toString();
    }

    public static class BusinessMapper extends Mapper<Object, Text, Text, NullWritable> {

        @Override
        public void map(final Object key, final Text input, final Context context)
                throws IOException, InterruptedException {
            // Get the Business string from json object
            String businessData = getDataText(input);

            context.write(new Text(businessData), NullWritable.get());
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Process Businesses");
        job.setJarByClass(ProcessBusinesses.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        job.setMapperClass(BusinessMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0] + "/businesses"));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/businesses"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new ProcessBusinesses(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}