package de.hpi.mmds.database;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;

import java.util.Collection;

public class MetadataRecord {
    private String asin;
    private float price;
    private String imUrl;
    private Related related;
    private LinkedTreeMap<String, Integer> salesRank;
    private String[][] categories;
    private String brand;

    public MetadataRecord(String asin, float price, String imUrl, Related related, LinkedTreeMap<String, Integer> salesRank,
                          String[][] categories, String brand) {
        this.asin = asin;
        this.price = price;
        this.imUrl = imUrl;
        this.related = related;
        this.salesRank = salesRank;
        this.categories = categories;
        this.brand = brand;
    }

    public String getAsin() {
        return asin;
    }

    public void setAsin(String asin) {
        this.asin = asin;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public String getImUrl() {
        return imUrl;
    }

    public void setImUrl(String imUrl) {
        this.imUrl = imUrl;
    }

    public Related getRelated() {
        return related;
    }

    public void setRelated(Related related) {
        this.related = related;
    }

    public LinkedTreeMap<String, Integer> getSalesRank() {
        return salesRank;
    }

    public void setSalesRank(LinkedTreeMap<String, Integer> salesRank) {
        this.salesRank = salesRank;
    }

    public String[][] getCategories() {
        return categories;
    }

    public void setCategories(String[][] categories) {
        this.categories = categories;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String toString() {
        Gson gson = new Gson();
        String json = gson.toJson(this);
        return json;
    }

    public class Related{
        public Collection<String> also_bought;
        public Collection<String> also_viewed;
        public Collection<String> bought_together;

        public Related(Collection<String> also_bought, Collection<String> also_viewed, Collection<String> bougt_together) {
            this.also_bought = also_bought;
            this.also_viewed = also_viewed;
            this.bought_together = bougt_together;
        }

        public Collection<String> getAlso_bought() {
            return also_bought;
        }

        public void setAlso_bought(Collection<String> also_bought) {
            this.also_bought = also_bought;
        }

        public Collection<String> getAlso_viewed() {
            return also_viewed;
        }

        public void setAlso_viewed(Collection<String> also_viewed) {
            this.also_viewed = also_viewed;
        }

        public Collection<String> getBougt_together() {
            return bought_together;
        }

        public void setBougt_together(Collection<String> bougt_together) {
            this.bought_together = bougt_together;
        }
    }
}
