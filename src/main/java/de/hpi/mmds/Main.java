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
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


public class Main {
    private final static String reviewPath = "resources/reviews";

    public static void main(String args[]) {


        SparkConf conf = new SparkConf();
        conf.setIfMissing("spark.master", "local[8]");
        conf.setAppName("mmds-amazon");
        JavaSparkContext context = new JavaSparkContext(conf);

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

            //Tuple2<String, Object>[] synonyms = model.findSynonyms("cable", 50);
            //System.out.println(Arrays.asList(synonyms).stream().map(a -> a._1).collect(Collectors.joining(", ")));

            JavaRDD<List<Tuple2<List<TaggedWord>, Integer>>> rddValuesRDD = tagRDD.map(
                    taggedWords -> BigramThesis.findKGramsEx(3, taggedWords)
            );

            JavaPairRDD<List<TaggedWord>, Integer> semiFinalRDD = rddValuesRDD.flatMapToPair(a -> a).reduceByKey(
                    (a, b) -> a + b);

            JavaPairRDD<Integer, List<TaggedWord>> swappedFinalRDD = semiFinalRDD.mapToPair(Tuple2::swap).sortByKey(
                    false);

            JavaPairRDD<List<TaggedWord>, Integer> finalRDD = swappedFinalRDD.mapToPair(Tuple2::swap);

            JavaPairRDD<List<VectorWithWords>, Integer> vectorRDD = finalRDD.mapToPair(a -> {
                List<VectorWithWords> vectors = a._1.stream().map(
                        taggedWord -> new VectorWithWords(model.transform(taggedWord.word()),
                                new ArrayList<>(Arrays.asList(taggedWord.word()))
                        )).collect(Collectors.toList());
                return new Tuple2<>(vectors, a._2);
            });

            JavaPairRDD<Tuple2<List<VectorWithWords>, Integer>, Tuple2<List<VectorWithWords>, Integer>> cartesianRDD =
                    vectorRDD.cartesian(vectorRDD).filter((t) -> compare(t._1._1, t._2._1));

            JavaPairRDD<Tuple2<List<VectorWithWords>, Integer>, Tuple2<List<VectorWithWords>, Integer>> resultsRDD =
                    cartesianRDD.reduceByKey(
                            (a, b) -> {
                                List<VectorWithWords> resultVector = a._1;
                                for (int i = 0; i < b._1.size(); i++) {
                                    resultVector.get(i).words.addAll(b._1.get(i).words);
                                }
                                return new Tuple2<>(resultVector, a._2 + b._2);
                            }
                    );


            /*List<Tuple2<List<VectorWithWords>, Integer>> vectorList = vectorRDD.collect();
            LinkedList<Tuple2<List<VectorWithWords>, Integer>> result = new LinkedList<>();
            for (Tuple2<List<VectorWithWords>, Integer> tuple : vectorList) {
                boolean a = true;
                for (int i = 0; i < result.size(); i++) {
                    if (compare(tuple._1, result.get(i)._1)) {
                        List<VectorWithWords> vectorWithWords = result.get(i)._1;
                        for (int j = 0; j < vectorWithWords.size(); j++) {
                            vectorWithWords.get(j).words.addAll(tuple._1.get(j).words);
                        }
                        result.addFirst(new Tuple2<>(vectorWithWords, tuple._2 + result.get(i)._2));
                        result.remove(i + 1);
                        a = false;
                    }
                }
                if (a) {
                    result.add(tuple);
                }
            }*/

            /*result.stream().sorted((a, b) -> b._2 - a._2).limit(50).forEach(
                    a -> {
                        a._1.stream().forEach(v -> System.out.print(mostCommon(v.words) + " , " + v.words + ", "));
                        System.out.print(a._2 + "\n");
                    });
            */
            System.out.println(resultsRDD.collect().stream().map((t) -> {
                return new Tuple2<>(
                        t._2._1.stream()
                                .map((s) -> s.words)
                                .collect(Collectors.toList()),
                        t._2._2);
            }).sorted((a, b) -> b._2 - a._2).limit(10).collect(Collectors.toList()));
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

    public static boolean compare(List<VectorWithWords> listVec1, List<VectorWithWords> listVec2) {
        double threshold = 0.9;
        double sum = 0;
        for (int i = 0; i < listVec1.size(); i++) {
            sum += cosineSimilarity(listVec1.get(i).vector.toArray(), listVec2.get(i).vector.toArray());
        }
        return threshold < (sum / listVec1.size());
    }

    public static <T> T mostCommon(List<T> list) {
        Map<T, Integer> map = new HashMap<>();

        for (T t : list) {
            Integer val = map.get(t);
            map.put(t, val == null ? 1 : val + 1);
        }

        Map.Entry<T, Integer> max = null;

        for (Map.Entry<T, Integer> e : map.entrySet()) {
            if (max == null || e.getValue() > max.getValue())
                max = e;
        }

        return max.getKey();
    }

    public static class VectorWithWords implements Serializable {
        public List<String> words;
        public Vector vector;

        public VectorWithWords(final Vector vector, final List<String> words) {
            this.words = words;
            this.vector = vector;
        }

        @Override
        public String toString() {
            return "VectorWithWords{" +
                    "words=" + words +
                    ", vector=" + vector +
                    '}';
        }
    }
}
