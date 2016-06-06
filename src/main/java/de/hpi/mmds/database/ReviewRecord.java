package de.hpi.mmds.database;

import com.google.gson.Gson;

import java.io.Serializable;

/**
 * Created by jaspar.mang on 02.05.16.
 */
public class ReviewRecord implements Serializable {
    public String reviewerID;
    public String asin;
    public String reviewerName;
    public int[] helpful;
    public String reviewText;
    public float overall;
    public String summary;
    public int unixReviewTime;
    public String reviewTime;

    public ReviewRecord(){

    }

    public ReviewRecord(String reviewerID, String asin, String reviewerName, int[] helpful, String reviewText, float overall, String summary, int unixReviewTime, String reviewTime) {
        this.reviewerID = reviewerID;
        this.asin = asin;
        this.reviewerName = reviewerName;
        this.helpful = helpful;
        this.reviewText = reviewText;
        this.overall = overall;
        this.summary = summary;
        this.unixReviewTime = unixReviewTime;
        this.reviewTime = reviewTime;
    }

    public String getReviewerID() {
        return reviewerID;
    }

    public void setReviewerID(String reviewerID) {
        this.reviewerID = reviewerID;
    }

    public String getAsin() {
        return asin;
    }

    public void setAsin(String asin) {
        this.asin = asin;
    }

    public String getReviewerName() {
        return reviewerName;
    }

    public void setReviewerName(String reviewerName) {
        this.reviewerName = reviewerName;
    }

    public int[] getHelpful() {
        return helpful;
    }

    public void setHelpful(int[] helpful) {
        this.helpful = helpful;
    }

    public String getReviewText() {
        return reviewText;
    }

    public void setReviewerText(String reviewText) {
        this.reviewText = reviewText;
    }

    public float getOverall() {
        return overall;
    }

    public void setOverall(float overall) {
        this.overall = overall;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public int getUnixReviewTime() {
        return unixReviewTime;
    }

    public void setUnixReviewTime(int unixReviewTime) {
        this.unixReviewTime = unixReviewTime;
    }

    public String getReviewTime() {
        return reviewTime;
    }

    public void setReviewTime(String reviewTime) {
        this.reviewTime = reviewTime;
    }

    public String toString() {
        Gson gson = new Gson();
        String json = gson.toJson(this);
        return json;
    }
}
