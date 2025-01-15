package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import kmeans.classes.Parameters;
import kmeans.classes.Point;

public class KmeansParallelK extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(KmeansParallelK.class);

	// Mapper to count total number of businesses (i.e. points)
	public static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private final static Text DUMMY_KEY = new Text("count"); // Used to send all businesses to the same reducer
		private final static LongWritable ONE = new LongWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split("-delimiter-");
			if (parts.length >= 6) {
				try {
					context.write(DUMMY_KEY, ONE);
				} catch (IOException | InterruptedException e) {
					logger.error(String.format("count mapper error for key %d", key.get()), e);
				}
			}
		}
	}

	// Reducer to count total number of businesses
	public static class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable value : values) {
				count += value.get();
			}
			context.write(key, new LongWritable(count));
		}
	}

	// Mapper to initialize Centroids
	public static class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			int minK = 3;
			int maxK = 6;
			Random rand = new Random(System.currentTimeMillis() + context.getTaskAttemptID().getId());
			int k = rand.nextInt(maxK - minK + 1) + minK;
			String distanceMeasure = rand.nextInt() % 2 == 0 ? "EU" : "MH";
			double samplingRate = 0.1;

			if (rand.nextDouble() < samplingRate) {
				String[] parts = value.toString().split("-delimiter-");
				if (parts.length >= 6) {
					// Only one reducer, send all points to the same reducer
					context.write(new Text(k + "," + distanceMeasure), new Text(parts[3] + " " + parts[4]));
				}
			}
		}
	}

	// Reducer to initialize centroids; we will only have 1 reducer
	public static class InitReducer extends Reducer<Text, Text, NullWritable, Text> {
		private int k;
		private Set<String> selectedPoints;
		private String keyString;

		@Override
		protected void setup(Context context) {
			selectedPoints = new HashSet<>();
			keyString = "";
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) {
			if (keyString.isEmpty()) {
				keyString = key.toString();
				k = Integer.parseInt(keyString.split(",")[0]);
			}
			for (Text point : values) {
				if (selectedPoints.size() < k) {
					selectedPoints.add(point.toString());
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for (String point : selectedPoints) {
				sb.append(point.toString()).append(",");
			}
			sb.deleteCharAt(sb.length() - 1);
			context.write(NullWritable.get(), new Text(keyString + "," + sb.toString()));
		}
	}

	public static class MergeMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(NullWritable.get(), value);
		}
	}

	public static class MergeReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
		@Override
		protected void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {

		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {
			if (value.toString().isEmpty())
				return;
			Parameters parameters = new Parameters(value.toString());
			context.write(new IntWritable(parameters.getK()), value);

		}
	}

	public static class KMeansReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

		private static List<Point> dataPoints = new ArrayList<>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {

			URI[] cacheFiles = context.getCacheFiles();

			if (cacheFiles != null && cacheFiles.length > 0) {

				Path cacheFilePath = new Path(cacheFiles[0]);
				FileSystem fs = cacheFilePath.getFileSystem(context.getConfiguration());

				// Read through the cached file
				try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(cacheFilePath)))) {
					String line;

					while ((line = reader.readLine()) != null) {
						String[] strs = line.split("-delimiter-");
						List<String> businessTypes = new ArrayList<>(Arrays.asList(strs[7].split("@")));
						Point point = new Point(strs[0], strs[1], strs[2], Double.parseDouble(strs[3]),
								Integer.parseInt(strs[4]), strs[5], strs[6], businessTypes,
								Integer.parseInt(strs[8]));
						dataPoints.add(point);
					}
				} catch (Exception e) {
					e.printStackTrace();
					throw new IOException("Error reading cached file", e);
				}
			}
		}

		@Override
		public void reduce(final IntWritable key, final Iterable<Text> values, final Context context)
				throws IOException, InterruptedException {
			HashMap<Double, HashMap<Point, List<Point>>> outputs = new HashMap<>();
			int k = 0;
			for (var val : values) {
				Parameters params = new Parameters(val.toString());
				k = params.getK();
				HashMap<Point, List<Point>> result = kmeans(params, dataPoints);
				Double sse = calculateSSE(result);
				outputs.put(sse, result);
			}
			double minSSE = Double.MAX_VALUE;
			for (Double sse : outputs.keySet()) {
				if (sse < minSSE) {
					minSSE = sse;
				}
			}

			HashMap<Point, List<Point>> result = outputs.get(minSSE);
			System.out.println(result.size() + " helloworld");
			StringBuilder output = new StringBuilder();
			output.append("Best Cluster for K value: ").append(k).append("\n");
			output.append("======================================\n\n");
			int clusterNumber = 1;

			// process result
			for (Point center : result.keySet()) {
				HashMap<String, Integer> businessTypeMap = new HashMap<>();
				HashMap<String, Integer> stateMap = new HashMap<>();
				HashMap<String, Integer> cityMap = new HashMap<>();
				List<Point> points = result.get(center);
				double totalPoints = points.size();
				double avgRating = 0;
				double avgNumOfReviews = 0;
				for (Point point : points) {
					avgRating += point.getRating();
					avgNumOfReviews += point.getNumOfReviews();
					// update statemap
					stateMap.merge(point.getState(), 1, Integer::sum);
					// update citymap
					cityMap.merge(point.getCity(), 1, Integer::sum);

					// update businessTypeMap
					for (String businessType : point.getBusinessTypes()) {
						businessTypeMap.merge(businessType, 1, Integer::sum);
					}
				}
				// Cluster features report
				// Add clear cluster separation
				output.append("Cluster ").append(clusterNumber).append("\n");
				output.append("------------------\n");

				// Add centroid information
				output.append("Centroid: (").append(center.getRating()).append(", ")
						.append(center.getPopularity()).append(")\n");

				// Add cluster statistics
				output.append("Size: ").append(totalPoints).append("\n");

				output.append("Centroid: ").append(center.getRating() + "," + center.getPopularity()).append("\n");
				output.append("Cluster Size: ").append(points.size()).append("\n");
				output.append("Average Rating: ").append(avgRating / totalPoints).append("\n");
				output.append("Average Number of Reviews: ").append(avgNumOfReviews / totalPoints).append("\n");
				output.append("\nTop Business Types:\n");
				businessTypeMap.entrySet().stream()
						.sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
						.limit(5)
						.forEach(e -> output.append(e.getKey())
								.append(": ")
								.append(e.getValue())
								.append("\n"));

				output.append("\nTop Cities:\n");
				cityMap.entrySet().stream()
						.sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
						.limit(5)
						.forEach(e -> output.append(e.getKey())
								.append(": ")
								.append(e.getValue())
								.append("\n"));

				output.append("\nTop States:\n");
				stateMap.entrySet().stream()
						.sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
						.limit(5)
						.forEach(e -> output.append(e.getKey())
								.append(": ")
								.append(e.getValue())
								.append("\n"));
				clusterNumber++;
			}

			context.write(NullWritable.get(), new Text(output.toString()));
		}
	}

	// Count total number of businesses
	private long countTotalPoints(String inputPath) throws Exception {
		Configuration conf = getConf();

		Job countJob = Job.getInstance(conf, "Count Total Points");
		final Configuration jobConf = countJob.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "-delimiter-");
		countJob.setJarByClass((KmeansParallelK.class));

		countJob.setMapperClass((CountMapper.class));
		countJob.setReducerClass((CountReducer.class));

		countJob.setOutputKeyClass(Text.class);
		countJob.setOutputValueClass(LongWritable.class);

		String countOutputPath = inputPath + "_count";
		FileInputFormat.addInputPath(countJob, new Path(inputPath + "/init"));
		FileOutputFormat.setOutputPath(countJob, new Path(countOutputPath));

		if (!countJob.waitForCompletion(true)) {
			throw new Exception("Count Total Points Job failed");
		}

		// Read the count from the output
		Path outputDir = new Path(countOutputPath);
		FileSystem fs = outputDir.getFileSystem(conf);
		long totalPoints = 0;

		FileStatus[] files = fs.listStatus(outputDir, path -> path.getName().startsWith("part-r-"));

		for (FileStatus file : files) {
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(fs.open(file.getPath())));
			String line = reader.readLine();
			if (line != null) {
				totalPoints += Long.parseLong(line.split("-delimiter-")[1]);
			}
			reader.close();
		}

		// clean up
		fs.delete(outputDir, true);

		return totalPoints;

	}

	// Initialize the centroids for the first run
	private void initializeCentroids(String inputPath, String outputPath, int n) throws Exception {
		int count = 0;
		// Count total number of businesses
		long totalPoints = countTotalPoints(inputPath);
		while (count < n) {

			Configuration conf = getConf();
			conf.setLong("kmeans.total.points", totalPoints);

			Job initJob = Job.getInstance(conf, "Initialize Centroids");

			initJob.setJarByClass(KmeansParallelK.class);

			initJob.setMapperClass(InitMapper.class);
			initJob.setReducerClass(InitReducer.class);

			initJob.setMapOutputKeyClass(Text.class);
			initJob.setMapOutputValueClass(Text.class);
			initJob.setOutputKeyClass(NullWritable.class);
			initJob.setOutputValueClass(Text.class);
			initJob.setNumReduceTasks(1);

			FileInputFormat.addInputPath(initJob, new Path(inputPath + "/init/"));
			FileOutputFormat.setOutputPath(initJob, new Path(inputPath + "/kmeans/" + count));

			if (!initJob.waitForCompletion(true)) {
				throw new Exception("Centroid initialization job failed");
			}
			count++;
		}
	}

	// Merging the centroid initialization into one file
	private void mergeInitializations(String inputPath, String mergedOutputPath, int n) throws Exception {
		Configuration conf = getConf();
		Job mergeJob = Job.getInstance(conf, "Merge Initializations");
		mergeJob.setJarByClass(KmeansParallelK.class);

		// Delete output path if exists
		Path outputPath = new Path(mergedOutputPath);
		FileSystem fs = outputPath.getFileSystem(conf);
		fs.delete(outputPath, true);

		// Set up the merge job
		mergeJob.setMapperClass(MergeMapper.class);
		mergeJob.setReducerClass(MergeReducer.class);
		mergeJob.setOutputKeyClass(NullWritable.class);
		mergeJob.setOutputValueClass(Text.class);

		// Add each part-r-00000 file explicitly
		for (int i = 0; i < n; i++) {
			FileInputFormat.addInputPath(mergeJob,
					new Path(inputPath + "/kmeans/" + i + "/part-r-00000"));
		}
		FileOutputFormat.setOutputPath(mergeJob, outputPath);

		if (!mergeJob.waitForCompletion(true)) {
			throw new Exception("Merge job failed");
		}
	}

	public static HashMap<Point, List<Point>> kmeans(Parameters parameter, List<Point> dataPoints) {

		// Get initialCenters from the parameter
		List<Point> initialCenters = parameter.getCenters();
		String distanceMeasure = parameter.getDistanceMeasure();
		// Map to store the Centroids and the points assigned to them
		HashMap<Point, List<Point>> centroids = new HashMap<>();

		// Initialize the map to store centroids and it's list of points
		for (Point p : initialCenters) {
			centroids.put(new Point(p.getRating(), p.getPopularity()), new ArrayList<Point>());
		}

		double SSE = 0; // initial SSE
		double SSEDiff = 2; // threshold for convergence

		// Until the threshold is not reached continue iterating
		while (SSEDiff > 0) {

			// Clear the centroid list from previous iteration
			for (Point c : centroids.keySet()) {
				centroids.get(c).clear();
			}

			// Assign each data point to it's closest centroid
			for (Point p : dataPoints) {
				Point minCentroid = null;
				Double minDist = Double.MAX_VALUE;

				for (Point c : centroids.keySet()) {
					Double dist = p.getDistance(c, distanceMeasure);
					if (dist < minDist) {
						minDist = dist;
						minCentroid = c;
					}
				}
				centroids.get(minCentroid).add(p);
			}

			// Get the list of current centroids
			List<Point> centroidList = new ArrayList<>(centroids.keySet());

			// Calculate the new centroid for each of the cluster
			for (Point c : centroidList) {
				List<Point> clusterPoints = centroids.get(c);
				Point newCentroid = getAverage(clusterPoints); // Find the new Centroid
				centroids.remove(c); // Remove the old centroid from the map
				centroids.put(newCentroid, clusterPoints); // Add the new centroid to the map
			}

			// Compute sum squared error difference between the current and old clusterings
			double newSSE = calculateSSE(centroids);
			SSEDiff = Math.abs(SSE - newSSE);
			SSE = newSSE;
		}

		return centroids;
	}

	// Helper method to get the average of a list of data points
	public static Point getAverage(List<Point> dataPoints) {
		double numOfDataPoints = 0;
		double ratingSum = 0;
		double popularitySum = 0;

		for (Point p : dataPoints) {
			ratingSum += p.getRating();
			popularitySum += p.getPopularity();
			numOfDataPoints++;
		}
		// Return the average of all the data points
		return new Point(ratingSum / numOfDataPoints, popularitySum / numOfDataPoints);
	}

	// Helper method top find the sum of squared error for the given clustering
	public static double calculateSSE(HashMap<Point, List<Point>> centroids) {
		double sse = 0;

		for (Point c : centroids.keySet()) {
			List<Point> points = centroids.get(c);

			for (Point p : points) {
				sse += Math.abs(
						Math.pow(c.getRating() - p.getRating(), 2)
								+ Math.pow(c.getPopularity() - p.getPopularity(), 2));
			}
		}

		return sse;
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(KmeansParallelK.class);

		int n = 50; // number of kmeans paramterrs

		String inputPath = args[0];
		String outputPath = args[1];

		// Initialize centroids
		initializeCentroids(inputPath, outputPath, n);

		// Merge initialization results using MapReduce
		String mergedPath = inputPath + "/merged";
		mergeInitializations(inputPath, mergedPath, n);

		// Use the merged file for the main job
		Path cacheFilePath = new Path(inputPath + "/init/processed-kmeans-input");
		job.addCacheFile(cacheFilePath.toUri());

		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// Set the input format class to NLineInputFormat
		job.setInputFormatClass(NLineInputFormat.class);
		// Set the number of lines per split
		NLineInputFormat.setNumLinesPerSplit(job, n);

		FileInputFormat.addInputPath(job, new Path(mergedPath + "/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		boolean res = job.waitForCompletion(true);

		// Clean up intermediate inputs
		Path mergedInputPath = new Path(inputPath + "/merged");
		Path kmeansInpuPath = new Path(inputPath + "/kmeans");
		FileSystem fs = cacheFilePath.getFileSystem(conf);
		fs.delete(mergedInputPath, true);
		fs.delete(kmeansInpuPath, true);

		return res ? 1 : 0;

	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new KmeansParallelK(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}