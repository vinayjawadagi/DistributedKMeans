package dp.classes;

import org.json.JSONObject;

public class Business {
    private String city;
    private String state;
    private String zipCode;
    private Double stars;
    private Integer reviewCount;
    private String businessId;
    private String name;
    private String categories;

    public Business(String jsonString) {
        // Get all the fields from the json object
        JSONObject json = new JSONObject(jsonString.toString());
        this.city = json.optString("city", "");
        this.state = json.optString("state", "");
        this.zipCode = json.optString("postal_code", "");
        this.stars = json.getDouble("stars");
        this.reviewCount = json.getInt("review_count");
        this.businessId = json.optString("business_id", "");
        this.name = json.optString("name", "");
        this.categories = json.optString("categories", "").replace(", ", "@");
    }

    public String toString() {
        return zipCode + "-joindelimiter-" + city + "-delimiter-" + state + "-delimiter-" + stars + "-delimiter-"
                + reviewCount + "-delimiter-" + businessId + "-delimiter-"
                + name + "-delimiter-" + categories;
    }
}
