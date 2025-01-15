// package kmeans.classes;

// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.List;

// public class KMeans {

// public static HashMap<Centroid, List<Point>> kmeans(Parameters parameter,
// List<Point> dataPoints) {

// // Get initialCenters from the parameter
// List<Point> initialCenters = parameter.getCenters();
// // Map to store the Centroids and the points assigned to them
// HashMap<Centroid, List<Point>> centroids = new HashMap<>();

// // Initialize the map to store centroids and it's list of points
// for (Point p : initialCenters) {
// centroids.put(new Centroid(p), new ArrayList<Point>());
// }

// double SSE = 0; // initial SSE
// double SSEDiff = 2; // threshold for convergence

// // Until the threshold is not reached continue iterating
// while (SSEDiff > 1) {

// // Clear the centroid list from previous iteration
// for (Centroid c : centroids.keySet()) {
// centroids.get(c).clear();
// }

// // Assign each data point to it's closest centroid
// for (Point p : dataPoints) {
// Centroid minCentroid = null;
// Double minDist = Double.MAX_VALUE;

// for (Centroid c : centroids.keySet()) {
// Double dist = p.getDistance(c);
// if (dist < minDist) {
// minDist = dist;
// minCentroid = c;
// }
// }
// centroids.get(minCentroid).add(p);
// }

// // Get the list of current centroids
// List<Centroid> centroidList = new ArrayList<>(centroids.keySet());

// // Calculate the new centroid for each of the cluster
// for (Centroid c : centroidList) {
// List<Point> clusterPoints = centroids.get(c);
// Centroid newCentroid = getAverage(clusterPoints); // Find the new Centroid
// centroids.remove(c); // Remove the old centroid from the map
// centroids.put(newCentroid, clusterPoints); // Add the new centroid to the map
// }

// // Compute sum squared error difference between the current and old
// clusterings
// double newSSE = calculateSSE(centroids);
// SSEDiff = Math.abs(SSE - newSSE);
// SSE = newSSE;
// }

// return centroids;
// }

// // Helper method to get the average of a list of data points
// public static Centroid getAverage(List<Point> dataPoints) {
// double numOfDataPoints = 0;
// double ratingSum = 0;
// double popularitySum = 0;

// for (Point p : dataPoints) {
// ratingSum += p.getRating();
// popularitySum += p.getPopularity();
// numOfDataPoints++;
// }
// // Return the average of all the data points
// return new Centroid(ratingSum / numOfDataPoints, popularitySum /
// numOfDataPoints);
// }

// // Helper method top find the sum of squared error for the given clustering
// public static double calculateSSE(HashMap<Centroid, List<Point>> centroids) {
// double sse = 0;

// for (Centroid c : centroids.keySet()) {
// List<Point> points = centroids.get(c);

// for (Point p : points) {
// sse += Math.abs(
// Math.pow(c.getRating() - p.getRating(), 2)
// + Math.pow(c.getPopularity() - p.getPopularity(), 2));
// }
// }

// return sse;
// }

// public static void main(String[] args) {
// // Generate dummy data points
// List<Point> dataPoints = new ArrayList<>();
// dataPoints.add(new Point(1.0, 2.0));
// dataPoints.add(new Point(2.0, 3.0));
// dataPoints.add(new Point(3.0, 4.0));
// dataPoints.add(new Point(4.0, 5.0));
// dataPoints.add(new Point(5.0, 6.0));
// dataPoints.add(new Point(6.0, 7.0));
// dataPoints.add(new Point(7.0, 8.0));
// dataPoints.add(new Point(8.0, 9.0));
// dataPoints.add(new Point(9.0, 10.0));
// dataPoints.add(new Point(10.0, 11.0));

// // Create parameters
// List<Point> centers = new ArrayList<>();
// centers.add(new Point(7.0, 8.0));
// centers.add(new Point(2.0, 3.0));
// Parameters parameters = new Parameters(1, "", 1, centers);

// // Call the kmeans method
// HashMap<Centroid, List<Point>> result = kmeans(parameters, dataPoints);

// // Print the result
// for (Centroid centroid : result.keySet()) {
// System.out.println("Centroid: " + centroid);
// System.out.println("Points: " + result.get(centroid));
// System.out.println();
// }
// }

// }
