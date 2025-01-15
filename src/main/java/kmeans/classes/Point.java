package kmeans.classes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Point implements Writable {

    // Business location fields
    private String zipcode;
    private String city;
    private String state;

    // Features fields for clustering
    private double rating;
    private int numOfReviews;
    private double popularity;

    // Other business fields
    private String businessId;
    private String businessName;
    private List<String> businessTypes;
    private int population;

    public Point(String zipcode, String city, String state, double rating, int numOfReviews, String businessId,
            String businessName, List<String> businessTypes, int population) {
        this.zipcode = zipcode;
        this.city = city;
        this.state = state;
        this.rating = rating;
        this.numOfReviews = numOfReviews;
        this.popularity = population > 0 ? numOfReviews / population : numOfReviews;
        this.businessId = businessId;
        this.businessName = businessName;
        this.businessTypes = businessTypes;
        this.population = population;
    }

    // Constructor for centroid
    public Point(double rating, double popularity) {
        this.rating = rating;
        this.popularity = popularity;
    }

    public Point() {
        this.businessTypes = new ArrayList<>();
    }

    // Copy constructor
    public Point(Point point) {
        this.zipcode = point.zipcode;
        this.city = point.city;
        this.state = point.state;
        this.rating = point.rating;
        this.numOfReviews = point.numOfReviews;
        this.popularity = point.popularity;
        this.businessId = point.businessId;
        this.businessName = point.businessName;
        this.businessTypes = new ArrayList<>(point.businessTypes);
        this.population = point.population;
    }

    public double getRating() {
        return this.rating;
    }

    public double getNumOfReviews() {
        return this.numOfReviews;
    }

    public double getPopularity() {
        return this.popularity;
    }

    public String getBusinessName() {
        return this.businessName;
    }

    public List<String> getBusinessTypes() {
        return this.businessTypes;
    }

    public String getCity() {
        return this.city;
    }

    public String getState() {
        return this.state;
    }

    public String getZipcode() {
        return this.zipcode;
    }

    public String getBusinessId() {
        return this.businessId;
    }

    public int getPopulation() {
        return this.population;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // Write location fields
        Text.writeString(out, zipcode);
        Text.writeString(out, city);
        Text.writeString(out, state);

        // Write clustering features
        out.writeDouble(rating);
        out.writeInt(numOfReviews);
        out.writeDouble(popularity);

        // Write business fields
        Text.writeString(out, businessId);
        Text.writeString(out, businessName);
        out.writeInt(businessTypes.size());
        for (String businessType : businessTypes) {
            Text.writeString(out, businessType);
        }
        out.writeInt(population);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        // Read location fields
        zipcode = Text.readString(in);
        city = Text.readString(in);
        state = Text.readString(in);

        // Read clustering features
        rating = in.readDouble();
        numOfReviews = in.readInt();
        popularity = in.readDouble();

        // Read business fields
        businessId = Text.readString(in);
        businessName = Text.readString(in);
        if (businessTypes == null) {
            businessTypes = new ArrayList<>();
        }
        businessTypes.clear();
        int businessTypesSize = in.readInt();
        for (int i = 0; i < businessTypesSize; i++) {
            businessTypes.add(Text.readString(in));
        }
        population = in.readInt();
    }

    // Calculate Euclidean distance between current Point and another point
    public double getDistance(Point other) {
        double ratingDiff = this.rating - other.rating;
        double popularityDiff = this.popularity - other.popularity;
        return Math.sqrt(ratingDiff * ratingDiff + popularityDiff * popularityDiff);
    }

    // Calculate distance between current Point and another point
    // (centroid)
    public double getDistance(Point other, String method) {

        double ratingDiff = this.rating - other.rating;
        double popularityDiff = this.popularity - other.popularity;

        if (method.equals("EU")) {
            return Math.sqrt(ratingDiff * ratingDiff + popularityDiff * popularityDiff);
        } else if (method.equals("MH")) {
            return Math.abs(ratingDiff) + Math.abs(popularityDiff);
        } else {
            throw new IllegalArgumentException("Invalid distance calculation method");
        }
    }

    @Override
    public String toString() {
        return String.format("%f-delimiter-%f",
                rating, popularity);
    }
}