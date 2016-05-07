package de.hpi.mmds;

import de.hpi.mmds.database.ReviewRecord;
import de.hpi.mmds.fileAccess.FileReader;
import de.hpi.mmds.nlp.TfIdf;

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

            /** Add the following lines to get a TFIDF measure **/
            /*
            TfIdf x = new TfIdf();
            for (ReviewRecord r : reviewRecordList) {
                x.addReviewText(r.getReviewText());
            }
            System.out.println(x.getTfIdf("The product does exactly as it should and is quite affordable.I did not " +
                    "realized it was double screened until it arrived, so it was even better than I had expected.As an " +
                    "added bonus, one of the screens carries a small hint of the smell of an old grape candy I used to " +
                    "buy, so for reminiscent's sake, I cannot stop putting the pop filter next to my nose and smelling " +
                    "it after recording. :DIf you needed a pop filter, this will work just as well as the expensive " +
                    "ones, and it may even come with a pleasing aroma like mine did!Buy this"));
            */
        }

        /*MetadataRecord metadataRecord = JsonReader.readMetadataJson(MetadataSample.JSON);
        //System.out.println(metadataRecord);

        ReviewRecord reviewRecord = JsonReader.readReviewJson(SampleReview.JSON);
        //System.out.println(reviewRecord);
        */
    }
}
