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
import java.util.LinkedList;
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
                    .setMinCount(0);
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

            List<Tuple2<List<Vector>, Integer>> vectorList = vectorRDD.collect();
            LinkedList<Tuple2<List<Vector>, Integer>> result = new LinkedList<>();
            vectorList.stream().forEach(tuple -> {
                boolean a = true;
                for (int i = 0; i < result.size(); i++) {

                    if (compare(tuple._1, result.get(i)._1)) {
                        result.addFirst(new Tuple2<>(tuple._1, tuple._2 + result.get(0)._2));
                        result.remove(i + 1);
                        a = false;
                    }
                }
                if (a) {
                    result.add(tuple);
                }
            });
            /*
            JavaPairRDD<Tuple2<List<Vector>, Integer>, Tuple2<List<Vector>, Integer>> cartesianRDD = vectorRDD
                    .cartesian(vectorRDD);

            */

            result.stream().limit(50).forEach(
                    a -> {
                        a._1.stream().forEach(v -> System.out.print(model.findSynonyms(v, 1)[0]._1 + " , "));
                        System.out.print(a._2 + "\n");
                    });

            System.out.println(finalRDD.take(10));
        }

    }

    public static double cosineSimilarity(double[] docVector1, double[] docVector2) {
        double dotProduct = 0.0;
        double magnitude1 = 0.0;
        double magnitude2 = 0.0;
        double cosineSimilarity = 0.0;

        for (int i = 0; i < docVector1.length; i++) //docVector1 and docVector2 must be of same length
        {
            dotProduct += docVector1[i] * docVector2[i];  //a.b
            magnitude1 += Math.pow(docVector1[i], 2);  //(a^2)
            magnitude2 += Math.pow(docVector2[i], 2); //(b^2)
        }

        magnitude1 = Math.sqrt(magnitude1);//sqrt(a^2)
        magnitude2 = Math.sqrt(magnitude2);//sqrt(b^2)

        if (magnitude1 != 0.0 | magnitude2 != 0.0) {
            cosineSimilarity = dotProduct / (magnitude1 * magnitude2);
        } else {
            return 0.0;
        }
        return cosineSimilarity;
    }

    public static boolean compare(List<Vector> listVec1, List<Vector> listVec2) {
        double threshold = 0.99;
        double sum = 0;
        for (int i = 0; i < listVec1.size(); i++) {
            sum += cosineSimilarity(listVec1.get(i).toArray(), listVec2.get(i).toArray());
        }
        return threshold < (sum / listVec1.size());
    }
}
