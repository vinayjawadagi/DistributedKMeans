package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import kmeans.classes.Point;

public class KmeansClustering extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(KmeansClustering.class);

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
	public static class InitMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
		@SuppressWarnings("unused")
		private int k;
		private Random rand;
		private double samplingRate;
		private static final Logger logger = LogManager.getLogger(KmeansClustering.class);

		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			k = conf.getInt("kmeans.k", 3);
			samplingRate = 0.1;
			rand = new Random(1);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (rand.nextDouble() < samplingRate) {
				String[] parts = value.toString().split("-delimiter-");
				if (parts.length >= 9) {
					try {
						// Ensure non-null values for required fields
						String zipcode = parts[0] != null ? parts[0] : "";
						String city = parts[1] != null ? parts[1] : "";
						String state = parts[2] != null ? parts[2] : "";
						double rating = Double.parseDouble(parts[3]);
						int reviews = Integer.parseInt(parts[4]);
						String businessId = parts[5] != null ? parts[5] : "";
						String businessName = parts[6] != null ? parts[6] : "";
						List<String> types = new ArrayList<>();
						if (parts[7] != null) {
							types.addAll(Arrays.asList(parts[7].split("@")));
						}
						int population = Integer.parseInt(parts[8]);

						Point point = new Point(zipcode, city, state, rating, reviews,
								businessId, businessName, types, population);
						context.write(new IntWritable(0), point);
					} catch (NumberFormatException e) {
						logger.error("Error parsing numeric values from line: " + value.toString());
					}
				}
			}
		}
	}

	// Reducer to initialize centroids; we will only have 1 reducer
	public static class InitReducer extends Reducer<IntWritable, Point, NullWritable, Text> {
		private int k;
		private List<Point> selectedPoints;
		private static final Logger logger = LogManager.getLogger(KmeansClustering.class);

		@Override
		protected void setup(Context context) {
			k = context.getConfiguration().getInt("kmeans.k", 3);
			selectedPoints = new ArrayList<>();
		}

		@Override
		protected void reduce(IntWritable key, Iterable<Point> values, Context context)
				throws IOException, InterruptedException {
			List<Point> allPoints = new ArrayList<>();

			for (Point point : values) {
				allPoints.add(new Point(point));
			}

			logger.info("InitReducer: Collected " + allPoints.size() + " points");
			Random rand = new Random(1);

			while (selectedPoints.size() < k && !allPoints.isEmpty()) {
				int index = rand.nextInt(allPoints.size());
				Point selected = allPoints.get(index);
				String output = String.format("%d-delimiter-%.6f-delimiter-%.6f",
						selectedPoints.size(),
						selected.getRating(),
						selected.getPopularity());
				logger.info("InitReducer: Writing centroid: " + output);
				context.write(NullWritable.get(), new Text(output));
				selectedPoints.add(selected);
				allPoints.remove(index);
			}
			logger.info("InitReducer: Selected " + selectedPoints.size() + " centroids");
		}
	}

	public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
		private List<Point> centroids;

		@Override
		protected void setup(Context context) throws IOException {
			// Load centroids from the distributed cache
			Configuration conf = context.getConfiguration();
			String[] centroidStrings = conf.getStrings("centroids");
			centroids = new ArrayList<>();

			if (centroidStrings == null) {
				logger.error("No centroids found in configuration");
				throw new IOException("No centroids found in configuration");
			}

			logger.info("Found " + centroidStrings.length + " centroids: " + Arrays.toString(centroidStrings));

			for (String centroidStr : centroidStrings) {
				String[] parts = centroidStr.split("-delimiter-");
				try {
					centroids.add(new Point(
							Double.parseDouble(parts[1]), // rating
							Double.parseDouble(parts[2]) // popularity
					));
				} catch (Exception e) {
					logger.error("Failed to parse centroid: " + centroidStr, e);
				}
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split("-delimiter-");
			// Verify we have all required fields
			if (parts.length < 9) {
				logger.warn("Skipping malformed record with insufficient fields: " + value.toString());
				context.getCounter("KMeans", "MalformedRecords").increment(1);
				return;
			}

			// Parse business types
			List<String> businessTypes = new ArrayList<>();
			if (parts[7] != null && !parts[7].isEmpty()) {
				businessTypes = Arrays.stream(parts[7].split("@"))
						.map(String::trim)
						.filter(s -> !s.isEmpty())
						.collect(Collectors.toList());
			}

			Point point = new Point(
					parts[0], // zipcode
					parts[1], // city
					parts[2], // state
					Double.parseDouble(parts[3]), // rating
					Integer.parseInt(parts[4]), // number of reviews
					parts[5], // business id
					parts[6], // business name
					businessTypes, // business types list
					Integer.parseInt(parts[8]) // population
			);

			// Find nearest centroid based on rating and popularity
			int nearestCentroidIndex = 0;
			double minDistance = Double.MAX_VALUE;

			for (int i = 0; i < centroids.size(); i++) {
				double distance = point.getDistance(centroids.get(i));
				if (distance < minDistance) {
					minDistance = distance;
					nearestCentroidIndex = i;
				}
			}

			context.write(new IntWritable(nearestCentroidIndex), point);
		}
	}

	public static class KMeansReducer extends Reducer<IntWritable, Point, NullWritable, Text> {

		@Override
		protected void reduce(IntWritable key, Iterable<Point> values, Context context)
				throws IOException, InterruptedException {

			double sumRating = 0;
			double sumPopularity = 0;
			int count = 0;

			// For analyzing cluster characteristics like type, city, and state
			Map<String, Integer> businessTypeCounts = new HashMap<>();
			Map<String, Integer> cityCounts = new HashMap<>();
			Map<String, Integer> stateCounts = new HashMap<>();

			// collect all points in the cluster
			List<Point> clusterPoints = new ArrayList<>();

			for (Point point : values) {
				sumRating += point.getRating();
				sumPopularity += point.getPopularity();
				count++;

				// Count business characteristics
				for (String type : point.getBusinessTypes()) {
					// increment type count
					businessTypeCounts.merge(type, 1, Integer::sum);
				}
				// increment city count
				cityCounts.merge(point.getCity(), 1, Integer::sum);
				// increment state count
				stateCounts.merge(point.getState(), 1, Integer::sum);

				clusterPoints.add(new Point(point)); // deep copy of point
			}

			// Calculate new centroid for the cluster
			if (count > 0) {
				Point newCentroid = new Point(
						sumRating / count, // average rating in cluster
						sumPopularity / count // average popularity in cluster
				);
				// Cluster features report
				StringBuilder output = new StringBuilder();
				String centroidOutput = String.format("%d\t%.6f\t%.6f",
						key.get(), newCentroid.getRating(), newCentroid.getPopularity());
				output.append("Centroid: ").append(centroidOutput).append("\n");
				output.append("Cluster Size: ").append(count).append("\n");

				output.append("\nBusiness Types:\n");
				businessTypeCounts.entrySet().stream()
						.sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
						.forEach(e -> output.append(e.getKey())
								.append(": ")
								.append(e.getValue())
								.append("\n"));

				output.append("\nTop Cities:\n");
				cityCounts.entrySet().stream()
						.sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
						.limit(5)
						.forEach(e -> output.append(e.getKey())
								.append(": ")
								.append(e.getValue())
								.append("\n"));

				output.append("\nTop States:\n");
				stateCounts.entrySet().stream()
						.sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
						.limit(5)
						.forEach(e -> output.append(e.getKey())
								.append(": ")
								.append(e.getValue())
								.append("\n"));

				context.write(NullWritable.get(), new Text(output.toString()));
			}
		}
	}

	// Count total number of businesses
	private long countTotalPoints(String inputPath) throws Exception {
		Configuration conf = getConf();

		// Read the count from the output
		String countOutputPath = inputPath + "_count";
		// FileSystem fs = FileSystem.get(conf);
		Path outputDir = new Path(countOutputPath);
		FileSystem fs = outputDir.getFileSystem(conf);
		fs.delete(outputDir, true);

		Job countJob = Job.getInstance(conf, "Count Total Points");
		final Configuration jobConf = countJob.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "-delimiter-");
		countJob.setJarByClass((KmeansClustering.class));

		countJob.setMapperClass((CountMapper.class));
		countJob.setReducerClass((CountReducer.class));

		countJob.setOutputKeyClass(Text.class);
		countJob.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(countJob, new Path(inputPath));
		FileOutputFormat.setOutputPath(countJob, new Path(countOutputPath));

		if (!countJob.waitForCompletion(true)) {
			throw new Exception("Count Total Points Job failed");
		}

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
	private void initializeCentroids(String inputPath, int k) throws Exception {
		// Count total points first
		long totalPoints = countTotalPoints(inputPath);

		// Delete _init directory if it exists
		Configuration conf = getConf();
		// FileSystem fs = FileSystem.get(conf);
		String initOutputPath = inputPath + "_init";
		Path initOutputPathObj = new Path(initOutputPath);
		FileSystem fs = initOutputPathObj.getFileSystem(conf);
		// fs.delete(new Path(initOutputPath), true);
		fs.delete(initOutputPathObj, true);

		// Configure and run init job
		conf.setInt("kmeans.k", k);
		conf.setLong("kmeans.total.points", totalPoints);

		Job initJob = Job.getInstance(conf, "Initialize Centroids");
		initJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "-delimiter-");
		initJob.setJarByClass(KmeansClustering.class);

		initJob.setMapperClass(InitMapper.class);
		initJob.setReducerClass(InitReducer.class);

		initJob.setMapOutputKeyClass(IntWritable.class);
		initJob.setMapOutputValueClass(Point.class);
		initJob.setOutputKeyClass(NullWritable.class);
		initJob.setOutputValueClass(Text.class);

		initJob.setNumReduceTasks(1);

		FileInputFormat.addInputPath(initJob, new Path(inputPath));
		FileOutputFormat.setOutputPath(initJob, new Path(initOutputPath));

		if (!initJob.waitForCompletion(true)) {
			throw new Exception("Centroid initialization job failed");
		}
	}

	private boolean checkConvergence(List<Point> oldCentroids, List<Point> newCentroids, double threshold) {
		if (oldCentroids.size() != newCentroids.size()) {
			return false;
		}

		double sseRating = 0.0;
		double ssePopularity = 0.0;

		for (int i = 0; i < oldCentroids.size(); i++) {
			Point oldCentroid = oldCentroids.get(i);
			Point newCentroid = newCentroids.get(i);

			double ratingDiff = oldCentroid.getRating() - newCentroid.getRating();
			double popularityDiff = oldCentroid.getPopularity() - newCentroid.getPopularity();

			sseRating += ratingDiff * ratingDiff;
			ssePopularity += popularityDiff * popularityDiff;
		}

		double totalSSE = sseRating + ssePopularity;

		return totalSSE < threshold;
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("Usage: make local <input_path> <output_path> <k> <maxIterations>");
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];
		int k = Integer.parseInt(args[2]);
		int maxIterations;
		try {
			maxIterations = Integer.parseInt(args[3]);
		} catch (NumberFormatException e) {
			maxIterations = 20; // default value
		}

		logger.info("Starting centroid initialization with k=" + k);
		logger.info("Starting centroid initialization with k=" + k);
		// initialize centroids
		initializeCentroids(inputPath, k);
		// After initialization job completes:
		logger.info("Centroid initialization job completed");

		// Read initial centroids
		Configuration conf = getConf();
		// FileSystem fs = FileSystem.get(conf);
		String initOutputPath = inputPath + "_init";
		Path initPath = new Path(initOutputPath);
		FileSystem fs = initPath.getFileSystem(conf);

		List<Point> currentCentroids = new ArrayList<>();
		final double convergenceThreshold = 0.01;
		FileStatus[] files = fs.listStatus(initPath, path -> path.getName().startsWith("part-r-"));
		logger.info("Found " + files.length + " output files");
		BufferedReader testReader = new BufferedReader(new InputStreamReader(fs.open(files[0].getPath())));

		String testLine;
		logger.info("Init file contents:");
		while ((testLine = testReader.readLine()) != null) {
			logger.info(testLine);
		}
		testReader.close();

		for (FileStatus file : files) {
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split("-delimiter-");
				if (parts.length >= 3) {
					currentCentroids.add(new Point(
							Double.parseDouble(parts[1]), // rating
							Double.parseDouble(parts[2]) // popularity
					));
				}
			}
			reader.close();
		}

		// Clean up initialization output
		fs.delete(initPath, true);

		// Add logging here
		logger.info("Current centroids size: " + currentCentroids.size());
		for (Point p : currentCentroids) {
			logger.info("Centroid: " + p.toString());
		}

		boolean converged = false;
		int iteration = 0;
		String lastIterationPath = null;
		List<Point> previousCentroids = null;

		// Main K-means iteration loop
		while (!converged && iteration < maxIterations) {
			previousCentroids = new ArrayList<>(currentCentroids);

			// Read new centroids from last iteration if available
			if (lastIterationPath != null) {

				List<Point> newCentroids = new ArrayList<>();
				files = fs.listStatus(new Path(lastIterationPath), path -> path.getName().startsWith("part-r-"));
				for (FileStatus file : files) {
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
					String line;
					while ((line = reader.readLine()) != null) {
						// Process line by line
						if (line.startsWith("Centroid: ")) {
							try {
								// Split the line and handle whitespace
								String[] tokens = line.substring(10).trim().split("\\s+");
								double rating = Double.parseDouble(tokens[0]);
								double popularity = Double.parseDouble(tokens[1]);
								newCentroids.add(new Point(rating, popularity));
								logger.info("Read centroid: rating=" + rating + ", popularity=" + popularity);
							} catch (Exception e) {
								logger.error("Failed to parse centroid line: " + line, e);
							}
						}
					}
					reader.close();
				}
				if (newCentroids.isEmpty()) {
					logger.error("No centroids read from iteration output");
				} else {
					logger.info("Read " + newCentroids.size() + " new centroids");
					currentCentroids = newCentroids;
				}
			}

			// Configure iteration job
			Configuration iterationConf = new Configuration(conf);
			String[] centroidStrings = new String[currentCentroids.size()];
			for (int i = 0; i < currentCentroids.size(); i++) {
				Point c = currentCentroids.get(i);
				centroidStrings[i] = String.format("%d-delimiter-%f-delimiter-%f",
						i, c.getRating(), c.getPopularity());
			}
			logger.info("Setting centroids: " + Arrays.toString(centroidStrings));
			iterationConf.setStrings("centroids", centroidStrings);

			// Set up and run iteration job
			String iterationOutput = outputPath + "_iteration_" + iteration;
			Path iterOutputPath = new Path(iterationOutput);
			FileSystem iterFs = iterOutputPath.getFileSystem(conf);
			iterFs.delete(iterOutputPath, true);
			Job iterationJob = Job.getInstance(iterationConf, "KMeans Iteration" + iteration);
			final Configuration jobConf = iterationJob.getConfiguration();
			jobConf.set("mapreduce.output.textoutputformat.separator", "-delimiter-");
			iterationJob.setJarByClass(KmeansClustering.class);

			iterationJob.setMapperClass(KMeansMapper.class);
			iterationJob.setReducerClass(KMeansReducer.class);

			iterationJob.setMapOutputKeyClass(IntWritable.class);
			iterationJob.setMapOutputValueClass(Point.class);
			iterationJob.setOutputKeyClass(IntWritable.class);
			iterationJob.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(iterationJob, new Path(inputPath));
			FileOutputFormat.setOutputPath(iterationJob, new Path(iterationOutput));

			if (!iterationJob.waitForCompletion(true)) {
				throw new Exception("KMeans Iteration " + iteration + " failed");
			}

			// Read new centroids
			List<Point> newCentroids = new ArrayList<>();
			files = fs.listStatus(new Path(iterationOutput), path -> path.getName().startsWith("part-r-"));

			for (FileStatus file : files) {
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
				String line;
				while ((line = reader.readLine()) != null) {
					if (line.startsWith("Centroid: ")) {
						String[] parts = line.substring(10).split("\\s+");
						List<String> values = Arrays.stream(parts)
								.filter(s -> !s.isEmpty())
								.collect(Collectors.toList());
						if (values.size() >= 2) {
							Point centroid = new Point(
									Double.parseDouble(values.get(0)),
									Double.parseDouble(values.get(1)));
							newCentroids.add(centroid);
						}
					}
				}
				reader.close();
			}

			// Update centroids and check convergence
			currentCentroids = newCentroids;
			logger.info("Updated centroids size: " + currentCentroids.size());
			for (Point p : currentCentroids) {
				logger.info("Updated centroid: " + p.toString());
			}

			if (previousCentroids != null) {
				converged = checkConvergence(previousCentroids, newCentroids, convergenceThreshold);
			}

			// Clean up and prepare for next iteration
			if (lastIterationPath != null) {
				Path lastIterationPathObj = new Path(lastIterationPath);
				FileSystem iterationFs = lastIterationPathObj.getFileSystem(conf);
				iterationFs.delete(lastIterationPathObj, true);
			}
			lastIterationPath = iterationOutput;
			iteration++;
		}

		// Rename final iteration output to desired output path
		if (lastIterationPath != null) {
			Path outputPathObj = new Path(outputPath);
			Path lastIterationPathObj = new Path(lastIterationPath);
			FileSystem finalFs = outputPathObj.getFileSystem(conf);
			finalFs.delete(outputPathObj, true); // Delete if exists
			finalFs.rename(lastIterationPathObj, outputPathObj);
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new KmeansClustering(), args);
		System.exit(exitCode);
	}
}