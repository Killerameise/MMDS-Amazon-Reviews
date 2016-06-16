package de.hpi.mmds;

import de.hpi.mmds.database.ReviewRecord;
import de.hpi.mmds.json.JsonReader;
import de.hpi.mmds.nlp.BigramThesis;
import de.hpi.mmds.nlp.TopicAnalysis;
import de.hpi.mmds.nlp.Utility;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.Word;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
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
        SQLContext sqlContext = new SQLContext(context);

        File folder = new File(reviewPath);
        File[] reviewFiles = folder.listFiles((dir, name) -> name.endsWith(".json"));

        for (File file : reviewFiles) {

            JavaRDD<String> fileRDD = context.textFile(file.getAbsolutePath());

            JavaRDD<ReviewRecord> recordsRDD = fileRDD.map(JsonReader::readReviewJson); //.sample(false, 0.001);

            JavaPairRDD<List<TaggedWord>, Float> tagRDD = recordsRDD.mapToPair(
                    (ReviewRecord x) -> new Tuple2(Utility.posTag(x.getReviewText()), x.getOverall()));

            tagRDD.cache();

            JavaRDD<List<String>> textRdd = tagRDD.map(
                    a -> a._1.stream().map(Word::word).collect(Collectors.toList())
            );

            // Learn a mapping from words to Vectors.
            Word2Vec word2Vec = new Word2Vec()
                    .setVectorSize(150)
                    .setMinCount(0);
            Word2VecModel model = word2Vec.fit(textRdd);

            //Tuple2<String, Object>[] synonyms = model.findSynonyms("cable", 50);
            //System.out.println(Arrays.asList(synonyms).stream().map(a -> a._1).collect(Collectors.joining(", ")));

            JavaRDD<List<Tuple2<List<TaggedWord>, Integer>>> rddValuesRDD = tagRDD.map(
                    taggedWords -> BigramThesis.findKGramsEx(3, taggedWords._1)
            );

            JavaPairRDD<List<TaggedWord>, Integer> semiFinalRDD = rddValuesRDD.flatMapToPair(a -> a).reduceByKey(
                    (a, b) -> a + b);
            semiFinalRDD.cache();

            JavaRDD<List<TaggedWord>> overallFeatures = semiFinalRDD.map(a -> a._1);
            Set<List<TaggedWord>> features = new HashSet<>(overallFeatures.collect());
            List<List<TaggedWord>> asd = new ArrayList<>(features.size());
            asd.addAll(features);
            Broadcast<List<List<TaggedWord>>> featureBroadcast = context.broadcast(asd);



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

            List<Tuple2<List<VectorWithWords>, Integer>> vectorList = vectorRDD.collect();

            LinkedList<Tuple2<List<VectorWithWords>, Integer>> result = new LinkedList<>();
            for (Tuple2<List<VectorWithWords>, Integer> tuple : vectorList) {
                boolean a = true;
                for (int i = 0; i < result.size(); i++) {
                    if (compare(tuple._1, result.get(i)._1)) {
                        List<VectorWithWords> vectorWithWords = result.get(i)._1;
                        for(int j = 0; j < vectorWithWords.size(); j++){
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
            }

            JavaRDD points = tagRDD.map(rating -> {
                List<List<TaggedWord>> bc = featureBroadcast.getValue();
                double[] v = new double[bc.size()];
                List<Tuple2<List<TaggedWord>, Integer>> output =  BigramThesis.findKGramsEx(3, rating._1);
                for (int i = 0; i < bc.size(); i++) {
                    for (Tuple2<List<TaggedWord>, Integer> tuple : output){
                        if (tuple._1.equals(bc.get(i))) {
                            v[i] = tuple._2;
                        }
                    }
                }
                return new LabeledPoint((double) (rating._2), Vectors.dense(v));
            });

            DataFrame training = sqlContext.createDataFrame(points, LabeledPoint.class);


            org.apache.spark.ml.regression.LinearRegression lr = new org.apache.spark.ml.regression.LinearRegression();

            lr.setMaxIter(10)
                    .setRegParam(0.01);

            LinearRegressionModel model1 = lr.train(training);

            System.out.println("Model 1 was fit using parameters: " + model1.coefficients());

            Map<List<TaggedWord>, Double> map = new HashMap<>();
            double[] coeffs = model1.coefficients().toArray();
            for (int i = 0; i < coeffs.length; i++) {
                map.put(asd.get(i), coeffs[i]);
            }

            Iterator<Map.Entry<List<TaggedWord>, Double>> i = Utility.valueIterator(map);
            System.out.println("Positive Words");
            int j = 0;
            while (j < 50) {
                Map.Entry<List<TaggedWord>, Double> entry = i.next();
                System.out.println(entry);
                j++;
            }
            System.out.println("");
            System.out.println("Negative Words");
            i = Utility.valueIteratorReverse(map);
            j = 0;
            while (j < 50) {
                Map.Entry<List<TaggedWord>, Double> entry = i.next();
                System.out.println(entry);
                j++;

            }
            /*
            JavaPairRDD<Tuple2<List<Vector>, Integer>, Tuple2<List<Vector>, Integer>> cartesianRDD = vectorRDD
                    .cartesian(vectorRDD);

            */

            result.stream().sorted((a, b) -> b._2 - a._2).limit(50).forEach(
                    a -> {
                        a._1.stream().forEach(v -> System.out.print(mostCommon(v.words) + " , " + v.words + ", "));
                        System.out.print(a._2 + "\n");
                    });

            System.out.println(overallFeatures);
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
        if (sum / listVec1.size() > 1.0) {
            System.out.println(sum + "   size: " + listVec1.size() + ", " + listVec2.size());
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
        public Vector       vector;

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
