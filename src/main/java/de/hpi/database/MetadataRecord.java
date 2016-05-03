package de.hpi.database;

import com.google.gson.Gson;

/**
 * Created by jaspar.mang on 02.05.16.
 */
public class MetadataRecord {
    public String asin;
    public float price;
    public String imUrl;
    public Related related;
    public SalesRank salesRank;
    public String[][] categories;

    public MetadataRecord(String asin, float price, String imUrl, Related related, SalesRank salesRank, String[][] categories) {
        this.asin = asin;
        this.price = price;
        this.imUrl = imUrl;
        this.related = related;
        this.salesRank = salesRank;
        this.categories = categories;
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

    public SalesRank getSalesRank() {
        return salesRank;
    }

    public void setSalesRank(SalesRank salesRank) {
        this.salesRank = salesRank;
    }

    public String[][] getCategories() {
        return categories;
    }

    public void setCategories(String[][] categories) {
        this.categories = categories;
    }

    public String toString() {
        Gson gson = new Gson();
        String json = gson.toJson(this);
        return json;
    }


    class SalesRank {
        public String toysGames;

        public SalesRank(String toysGames) {
            this.toysGames = toysGames;
        }

        public String getToysGames() {
            return toysGames;
        }

        public void setToysGames(String toysGames) {
            this.toysGames = toysGames;
        }
    }

    class Related{
        public String [] also_bought;
        public String [] also_viewed;
        public String [] bougt_together;

        public Related(String[] also_bought, String[] also_viewed, String[] bougt_together) {
            this.also_bought = also_bought;
            this.also_viewed = also_viewed;
            this.bougt_together = bougt_together;
        }

        public String[] getAlso_bought() {
            return also_bought;
        }

        public void setAlso_bought(String[] also_bought) {
            this.also_bought = also_bought;
        }

        public String[] getAlso_viewed() {
            return also_viewed;
        }

        public void setAlso_viewed(String[] also_viewed) {
            this.also_viewed = also_viewed;
        }

        public String[] getBougt_together() {
            return bougt_together;
        }

        public void setBougt_together(String[] bougt_together) {
            this.bougt_together = bougt_together;
        }
    }
}
