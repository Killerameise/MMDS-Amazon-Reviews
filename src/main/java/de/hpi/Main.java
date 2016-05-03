package de.hpi;

import de.hpi.database.MetadataRecord;
import de.hpi.database.ReviewRecord;
import de.hpi.fileAccess.FileReader;
import de.hpi.json.JsonReader;
import de.hpi.json.sample.MetadataSample;
import de.hpi.json.sample.SampleReview;

import java.util.List;

/**
 * Created by jaspar.mang on 02.05.16.
 */
public class Main {
    final static String file = "Path/reviews_Amazon_Instant_Video_5.json";

    public static void main(String args[]) {

        final FileReader fileReader = new FileReader(file);
        List<ReviewRecord> reviewRecordList = fileReader.readReviewsFromFile();
        for (ReviewRecord reviewRecord : reviewRecordList) {
            System.out.println(reviewRecord);
        }

        MetadataRecord metadataRecord = JsonReader.readMetadataJson(MetadataSample.JSON);
        System.out.println(metadataRecord);

        ReviewRecord reviewRecord = JsonReader.readReviewJson(SampleReview.JSON);
        System.out.println(reviewRecord);
    }
}
