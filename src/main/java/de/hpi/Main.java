package de.hpi;

import de.hpi.database.Review;
import de.hpi.fileAccess.FileReader;
import de.hpi.json.JsonReader;
import de.hpi.json.sample.SampleReview;

import java.util.List;

/**
 * Created by jaspar.mang on 02.05.16.
 */
public class Main {
    final static String file = "Path/reviews_Amazon_Instant_Video_5.json";

    public static void main(String args[]) {
        final FileReader fileReader = new FileReader(file);
        List<Review> reviewList = fileReader.readFile();
        for (Review review : reviewList) {
            System.out.println(review);
        }

        Review review = JsonReader.readJson(SampleReview.JSON);
        System.out.println(review);
    }
}
