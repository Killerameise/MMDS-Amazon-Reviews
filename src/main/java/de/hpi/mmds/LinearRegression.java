package de.hpi.mmds;

import de.hpi.mmds.database.ReviewRecord;
import de.hpi.mmds.fileAccess.FileReader;
import de.hpi.mmds.nlp.TopicAnalysis;
import de.hpi.mmds.nlp.Utility;
import edu.stanford.nlp.ling.TaggedWord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.File;
import java.io.FilenameFilter;
import java.util.*;

/**
 * Created by axel on 14.06.16.
 */
public class LinearRegression {

    private final static String reviewPath = "resources/reviews";

    public static void main(String args[]) {

        SparkConf conf = new SparkConf();
        conf.set("spark.master", "local[8]");
        conf.setAppName("mmds-amazon");
        JavaSparkContext context = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);
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

            JavaRDD<ReviewRecord> recordsRDD = context.parallelize(reviewRecordList);

            JavaRDD<Tuple2<Float, List<TaggedWord>>> ratings = recordsRDD.map(
                    (ReviewRecord r) ->
                            new Tuple2<Float, List<TaggedWord>>(r.getOverall(), Utility.posTag(r.getReviewText())));


            TopicAnalysis ta = new TopicAnalysis();
            for (ReviewRecord r : reviewRecordList) {
                ta.findWords(Utility.posTag(r.getReviewText()));
            }
            System.out.println(ta.wordCounter.size());
            ta.removeLowFrequencyTerms(10);
            String[] overallFeatures = ta.getCounterAsStringArray();
            HashMap<String, Integer> wordToFeatureIndex = ta.wordToFeaturePos;
            System.out.println(overallFeatures.length);
            System.out.println(wordToFeatureIndex.size());


            JavaRDD points = ratings.map(rating -> {
                double[] v = new double[overallFeatures.length];
                TopicAnalysis x = new TopicAnalysis();
                x.findWords(rating._2);
                String[] features = x.getCounterAsStringArray();
                for (int i = 0; i < features.length; i++) {
                    if (wordToFeatureIndex.containsKey(features[i])) {
                        int pos = wordToFeatureIndex.get(features[i]);
                        v[pos] = Double.parseDouble(String.valueOf(x.wordCounter.getCount(features[i])));
                    }
                }
                return new LabeledPoint((double) (rating._1), Vectors.dense(v));
            });

            DataFrame training = sqlContext.createDataFrame(points, LabeledPoint.class);


            org.apache.spark.ml.regression.LinearRegression lr = new org.apache.spark.ml.regression.LinearRegression();

            lr.setMaxIter(10)
                    .setRegParam(0.01);

            LinearRegressionModel model1 = lr.train(training);

            System.out.println("Model 1 was fit using parameters: " + model1.coefficients());


            TreeMap<String, Double> map = new TreeMap<>();
            double[] coeffs = model1.coefficients().toArray();
            for (int i = 0; i < coeffs.length; i++) {
                map.put(overallFeatures[i], coeffs[i]);
            }

            Iterator<Map.Entry<String, Double>> i = Utility.valueIterator(map);
            System.out.println("Positive Words");
            int j = 0;
            while (j < 50) {
                Map.Entry<String, Double> entry = i.next();
                if (ta.wordCounter.getCount(entry.getKey()) > 15) {
                    System.out.println(entry);
                    j++;
                }
            }
            System.out.println("");
            System.out.println("Negative Words");
            i = Utility.valueIteratorReverse(map);
            j = 0;
            while (j < 50) {
                Map.Entry<String, Double> entry = i.next();
                if (ta.wordCounter.getCount(entry.getKey()) > 15) {
                    System.out.println(entry);
                    j++;
                }
            }
        }
    }
}
