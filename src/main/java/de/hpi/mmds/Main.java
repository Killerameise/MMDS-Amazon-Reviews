package de.hpi.mmds;

import de.hpi.mmds.database.ReviewRecord;
import de.hpi.mmds.json.JsonReader;
import de.hpi.mmds.nlp.BigramThesis;
import de.hpi.mmds.nlp.Utility;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.Word;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;


public class Main {
    private final static String reviewPath = "resources/reviews";

    public static void main(String args[]) {


        SparkConf conf = new SparkConf();
        conf.setIfMissing("spark.master", "local[8]");
        conf.setAppName("mmds-amazon");
        JavaSparkContext context = new JavaSparkContext(conf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(context);

        File folder = new File(reviewPath);
        File[] reviewFiles = folder.listFiles((dir, name) -> name.endsWith(".json"));

        for (File file : reviewFiles) {

            JavaRDD<String> fileRDD = context.textFile(file.getAbsolutePath());

            JavaRDD<ReviewRecord> recordsRDD = fileRDD.map(JsonReader::readReviewJson);

            JavaRDD<List<TaggedWord>> tagRDD = recordsRDD.map(
                    (r) -> Utility.posTag(r.getReviewText())
            );

            tagRDD.cache();

            JavaRDD<List<String>> textRdd = tagRDD.map(
                    a -> a.stream().map(Word::word).collect(Collectors.toList())
            );

            // Learn a mapping from words to Vectors.
            Word2Vec word2Vec = new Word2Vec()
                    .setVectorSize(150)
                    .setMinCount(5);
            Word2VecModel model = word2Vec.fit(textRdd);

            Tuple2<String, Object>[] synonyms = model.findSynonyms("cable", 10);
            System.out.println(synonyms);

            JavaRDD<List<Tuple2<List<TaggedWord>, Integer>>> rddValuesRDD = tagRDD.map(
                    taggedWords -> BigramThesis.findKGramsEx(3, taggedWords)
            );

            JavaPairRDD<List<TaggedWord>, Integer> semiFinalRDD = rddValuesRDD.flatMapToPair(a -> a).reduceByKey(
                    (a, b) -> a + b);

            JavaPairRDD<Integer, List<TaggedWord>> swappedFinalRDD = semiFinalRDD.mapToPair(Tuple2::swap).sortByKey(
                    false);

            JavaPairRDD<List<TaggedWord>, Integer> finalRDD = swappedFinalRDD.mapToPair(Tuple2::swap);

            JavaPairRDD<List<Vector>, Integer> vectorRDD = finalRDD.mapToPair(a -> {
                List<Vector> vectors = a._1.stream().map(taggedWord -> model.transform(taggedWord.word())).collect(
                        Collectors.toList());
                return new Tuple2<>(vectors, a._2);
            });

            /*vectorRDD.reduce(a -> {
                return a;
            });*/

            System.out.println(finalRDD.take(10));
        }
    }
}
