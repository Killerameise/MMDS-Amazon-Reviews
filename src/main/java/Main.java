import Sample.SampleReview;

/**
 * Created by jaspar.mang on 02.05.16.
 */
public class Main {
    public static void main(String args[]){
        Review review  = JsonReader.readJson(SampleReview.JSON);
        System.out.println(review);
    }
}
