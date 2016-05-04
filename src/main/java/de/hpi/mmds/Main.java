package de.hpi.mmds;

import de.hpi.mmds.database.MetadataRecord;
import de.hpi.mmds.database.ReviewRecord;
import de.hpi.mmds.fileAccess.FileReader;
import de.hpi.mmds.json.JsonReader;
import de.hpi.mmds.json.sample.MetadataSample;
import de.hpi.mmds.json.sample.SampleReview;

import java.io.File;
import java.io.FilenameFilter;
import java.util.List;

/**
 * Created by jaspar.mang on 02.05.16.
 */
public class Main {
    private final static String reviewPath = "resources/reviews";

    public static void main(String args[]) {

        File folder = new File(reviewPath);
        File[] reviewFiles = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".json");
            }
        });

        for (File file : reviewFiles) {
            final FileReader fileReader = new FileReader(file.getAbsolutePath());
            List<ReviewRecord> reviewRecordList = fileReader.readReviewsFromFile();
            System.out.println(reviewRecordList.size());
        }

        MetadataRecord metadataRecord = JsonReader.readMetadataJson(MetadataSample.JSON);
        System.out.println(metadataRecord);

        ReviewRecord reviewRecord = JsonReader.readReviewJson(SampleReview.JSON);
        System.out.println(reviewRecord);
    }
}
