/**
 * Created by jaspar.mang on 02.05.16.
 */
public class Review {
    public String reviewerID;
    public String asin;
    public String reviewerName;
    public int[] helpful;
    public String reviewerText;
    public float overall;
    public String summary;
    public int unixReviewTime;
    public String reviewTime;

    public Review(String reviewerID, String asin, String reviewerName, int[] helpful, String reviewerText, float overall, String summary, int unixReviewTime, String reviewTime) {
        this.reviewerID = reviewerID;
        this.asin = asin;
        this.reviewerName = reviewerName;
        this.helpful = helpful;
        this.reviewerText = reviewerText;
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

    public String getReviewerText() {
        return reviewerText;
    }

    public void setReviewerText(String reviewerText) {
        this.reviewerText = reviewerText;
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

    /*{
  "reviewerID": "A2SUAM1J3GNN3B",
  "asin": "0000013714",
  "reviewerName": "J. McDonald",
  "helpful": [2, 3],
  "reviewText": "I bought this for my husband who plays the piano.  He is having a wonderful time playing these old hymns.  The music  is at times hard to read because we think the book was published for singing from more than playing from.  Great purchase though!",
  "overall": 5.0,
  "summary": "Heavenly Highway Hymns",
  "unixReviewTime": 1252800000,
  "reviewTime": "09 13, 2009"
}*/
}
