package kmeans.classes;

import java.util.ArrayList;
import java.util.List;

public class Parameters {
    private int k;
    private String distanceMeasure;
    private List<Point> centers;

    public Parameters(int k, String distanceMeasure, List<Point> centers) {
        this.k = k;
        this.distanceMeasure = distanceMeasure;
        this.centers = centers;
    }

    public Parameters(String paraString) {
        String[] strs = paraString.split(",");
        this.k = Integer.parseInt(strs[0]);
        this.distanceMeasure = strs[1];
        this.centers = new ArrayList<>();
        for (int i = 2; i < strs.length; i++) {
            String[] dimensions = strs[i].split(" ");
            centers.add(new Point(Double.parseDouble(dimensions[0]), Double.parseDouble(dimensions[1])));
        }
    }

    public List<Point> getCenters() {
        return this.centers;
    }

    public String getDistanceMeasure() {
        return this.distanceMeasure;
    }

    public int getK() {
        return this.k;
    }
}
