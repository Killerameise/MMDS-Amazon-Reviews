package de.hpi.mmds;

import de.hpi.mmds.database.ReviewRecord;
import de.hpi.mmds.json.JsonReader;
import de.hpi.mmds.nlp.BigramThesis;
import de.hpi.mmds.nlp.Utility;
import de.hpi.mmds.nlp.template.AdjectiveNounTemplate;
import de.hpi.mmds.nlp.template.Template;
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
import scala.Tuple3;

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


            Template template = new AdjectiveNounTemplate();
            JavaRDD<List<Tuple2<List<TaggedWord>, Integer>>> rddValuesRDD = tagRDD.map(
                    taggedWords -> BigramThesis.findKGramsEx(3, taggedWords, template)
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

            List<Tuple3<List<VectorWithWords>, Integer, Set<List<String>>>> clusters = vectorRDD.aggregate(
                    new LinkedList<>(),
                    (List<Tuple3<List<VectorWithWords>, Integer, Set<List<String>>>> acc, Tuple2<List<VectorWithWords>, Integer> value) -> {
                        Boolean foundOne = false;
                        List<Tuple3<List<VectorWithWords>, Integer, Set<List<String>>>> new_acc = new LinkedList<>(acc);
                        for (int i = 0; i<acc.size(); i++){
                            Tuple3<List<VectorWithWords>, Integer, Set<List<String>>> l = acc.get(i);
                            if(compare(value._1(), l._1())){
                                new_acc.remove(i);
                                Set<List<String>> words = new HashSet<>(l._3());
                                List<String> w2 = new LinkedList<>();
                                value._1().forEach(x -> w2.addAll(x.words));
                                words.add(w2);
                                new_acc.add(new Tuple3<>(l._1(), l._2() + value._2(), words));
                                foundOne = true;
                                break;
                            }
                        }
                        if (!foundOne){
                            Set<List<String>> words = new HashSet<>();
                            List<String> w2 = new LinkedList<>();
                            value._1().forEach(x -> w2.addAll(x.words));
                            words.add(w2);
                            new_acc.add(new Tuple3<>(value._1(), value._2(), words));
                        }
                        return new_acc;

                    },
                    (List<Tuple3<List<VectorWithWords>, Integer, Set<List<String>>>> acc1, List<Tuple3<List<VectorWithWords>, Integer, Set<List<String>>>> acc2) -> {
                        List<Tuple3<List<VectorWithWords>, Integer, Set<List<String>>>> dotProduct = new LinkedList<>();
                        List<Tuple3<List<VectorWithWords>, Integer, Set<List<String>>>> result = new LinkedList<>();
                        dotProduct.addAll(acc1);
                        dotProduct.addAll(acc2);
                        for (int i = 0; i< dotProduct.size(); i++){
                            Boolean foundOne = false;
                            Tuple3<List<VectorWithWords>, Integer, Set<List<String>>> l1 = dotProduct.get(i);
                            for (int j = i+1; j< dotProduct.size(); j++){
                                Tuple3<List<VectorWithWords>, Integer, Set<List<String>>> l2 = dotProduct.get(j);
                                if(compare(l1._1(), l2._1())){
                                    Set<List<String>> words =  new HashSet<>(l1._3());
                                    words.addAll(l2._3());
                                    result.add(new Tuple3<>(l1._1(), l1._2()+l2._2(), words));
                                    foundOne = true;
                                    break;
                                }
                            }
                            if (!foundOne){
                                result.add(new Tuple3<>(l1._1(), l1._2(), l1._3()));
                            }
                        }
                        return result;
                    }
            );


            clusters.sort((a, b) -> b._2() - a._2());
            clusters.stream().limit(25).forEach((t) -> {
                List<String> representation = t._1().stream().map((s) -> s.words.get(0)).collect(Collectors.toList());
                System.out.println(representation.toString() + ": " + t._2().toString() + " | " + t._3().toString());
            });
            System.out.println(clusters.size());

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
