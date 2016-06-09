package de.hpi.mmds;

import de.hpi.mmds.database.ReviewRecord;
import de.hpi.mmds.fileAccess.FileReader;
import de.hpi.mmds.nlp.BigramThesis;
import de.hpi.mmds.nlp.Utility;
import edu.stanford.nlp.ling.TaggedWord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.FilenameFilter;
import java.util.List;


public class Main {
    private final static String reviewPath = "resources/reviews";

    public static void main(String args[]) {

        SparkConf conf = new SparkConf();
        conf.setIfMissing("spark.master", "local[2]");
        conf.setAppName("mmds-amazon");
        JavaSparkContext context = new JavaSparkContext(conf);

        File folder = new File(reviewPath);
        File[] reviewFiles = folder.listFiles((dir, name) -> name.endsWith(".json"));

        for (File file : reviewFiles) {

            BigramThesis bt = new BigramThesis();
            final FileReader fileReader = new FileReader(file.getAbsolutePath());
            List<ReviewRecord> reviewRecordList = fileReader.readReviewsFromFile();

            JavaRDD<ReviewRecord> recordsRDD = context.parallelize(reviewRecordList);
            JavaRDD<List<TaggedWord>> textRDD = recordsRDD.map(
                    (r) -> Utility.posTag(r.getReviewText())
            );

            JavaRDD<List<Tuple2<List<TaggedWord>, Integer>>> rddValuesRDD = textRDD.map(
                    taggedWords -> BigramThesis.findKGramsEx(3, taggedWords)
            );

            List<Tuple2<List<TaggedWord>, Integer>> rddValues = rddValuesRDD.reduce(
                    (a, b) -> {a.addAll(b); return a;}
            );

            JavaPairRDD<List<TaggedWord>, Integer> semiFinalRDD = context.parallelizePairs(rddValues).reduceByKey((a, b) -> a + b);

            JavaPairRDD<Integer, List<TaggedWord>> swappedFinalRDD = semiFinalRDD.mapToPair(Tuple2::swap).sortByKey(false);

            JavaPairRDD<List<TaggedWord>, Integer> finalRDD = swappedFinalRDD.mapToPair(Tuple2::swap);

            System.out.println(finalRDD.take(10));


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
        //System.out.println(bt.bigramCounter);


        /*MetadataRecord metadataRecord = JsonReader.readMetadataJson(MetadataSample.JSON);
        //System.out.println(metadataRecord);

        ReviewRecord reviewRecord = JsonReader.readReviewJson(SampleReview.JSON);
        //System.out.println(reviewRecord);
        */
    }
}
