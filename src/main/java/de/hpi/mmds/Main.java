package de.hpi.mmds;

import de.hpi.mmds.database.ReviewRecord;
import de.hpi.mmds.json.JsonReader;
import de.hpi.mmds.nlp.BigramThesis;
import de.hpi.mmds.nlp.Utility;
import de.hpi.mmds.nlp.template.AdjectiveNounTemplate;
import de.hpi.mmds.nlp.template.NounNounTemplate;
import de.hpi.mmds.nlp.template.Template;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.Word;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
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
import scala.Tuple3;

import javax.swing.text.html.HTML;
import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


public class Main {
    private static String reviewPath = "resources/reviews";

    public static void main(String args[]) {


        SparkConf conf = new SparkConf();
        conf.setIfMissing("spark.master", "local[8]");
        conf.setAppName("mmds-amazon");
        JavaSparkContext context = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);

        if (args.length == 1) {
            reviewPath = args[0];
        } else {
            File folder = new File(reviewPath);
            File[] reviewFiles = folder.listFiles((dir, name) -> name.endsWith(".json"));
            reviewPath = reviewFiles[0].getAbsolutePath();
        }

        JavaRDD<String> fileRDD = context.textFile(reviewPath);

        JavaRDD<ReviewRecord> recordsRDD = fileRDD.map(JsonReader::readReviewJson);

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


        Template template = new NounNounTemplate();
        JavaRDD<List<Tuple2<List<TaggedWord>, Integer>>> rddValuesRDD = tagRDD.map(
                taggedWords -> BigramThesis.findKGramsEx(3, taggedWords._1(), template)
        );

        JavaPairRDD<List<TaggedWord>, Integer> semiFinalRDD = rddValuesRDD.flatMapToPair(a -> a).reduceByKey(
                (a, b) -> a + b);

        JavaPairRDD<Integer, List<TaggedWord>> swappedFinalRDD = semiFinalRDD.mapToPair(Tuple2::swap).sortByKey(
                false);

        JavaPairRDD<List<TaggedWord>, Integer> finalRDD = swappedFinalRDD.mapToPair(Tuple2::swap);

        JavaPairRDD<List<VectorWithWords>, Integer> vectorRDD = finalRDD.mapToPair(a -> {
            List<VectorWithWords> vectors = a._1.stream().map(
                    taggedWord -> new VectorWithWords(model.transform(taggedWord.word()),
                            new ArrayList<>(Arrays.asList(taggedWord)), template
                    )).collect(Collectors.toList());
            return new Tuple2<>(vectors, a._2);
        });

        List<Tuple3<List<VectorWithWords>, Integer, Set<List<TaggedWord>>>> clusters = vectorRDD.aggregate(
                new LinkedList<>(),
                (List<Tuple3<List<VectorWithWords>, Integer, Set<List<TaggedWord>>>> acc, Tuple2<List<VectorWithWords>, Integer> value) -> {
                    Boolean foundOne = false;
                    List<Tuple3<List<VectorWithWords>, Integer, Set<List<TaggedWord>>>> new_acc = new LinkedList<>(acc);
                    for (int i = 0; i<acc.size(); i++){
                        Tuple3<List<VectorWithWords>, Integer, Set<List<TaggedWord>>> l = acc.get(i);
                        if(compare(value._1(), l._1())){
                            new_acc.remove(i);
                            Set<List<TaggedWord>> words = new HashSet<>(l._3());
                            List<TaggedWord> w2 = new LinkedList<>();
                            value._1().forEach(x -> w2.addAll(x.words));
                            words.add(w2);
                            new_acc.add(new Tuple3<>(l._1(), l._2() + value._2(), words));
                            foundOne = true;
                            break;
                        }
                    }
                    if (!foundOne){
                        Set<List<TaggedWord>> words = new HashSet<>();
                        List<TaggedWord> w2 = new LinkedList<>();
                        value._1().forEach(x -> w2.addAll(x.words));
                        words.add(w2);
                        new_acc.add(new Tuple3<>(value._1(), value._2(), words));
                    }
                    return new_acc;

                },
                (List<Tuple3<List<VectorWithWords>, Integer, Set<List<TaggedWord>>>> acc1, List<Tuple3<List<VectorWithWords>, Integer, Set<List<TaggedWord>>>> acc2) -> {
                    List<Tuple3<List<VectorWithWords>, Integer, Set<List<TaggedWord>>>> dotProduct = new LinkedList<>();
                    List<Tuple3<List<VectorWithWords>, Integer, Set<List<TaggedWord>>>> result = new LinkedList<>();
                    dotProduct.addAll(acc1);
                    dotProduct.addAll(acc2);
                    for (int i = 0; i< dotProduct.size(); i++){
                        Boolean foundOne = false;
                        Tuple3<List<VectorWithWords>, Integer, Set<List<TaggedWord>>> l1 = dotProduct.get(i);
                        for (int j = i+1; j< dotProduct.size(); j++){
                            Tuple3<List<VectorWithWords>, Integer, Set<List<TaggedWord>>> l2 = dotProduct.get(j);
                            if(compare(l1._1(), l2._1())){
                                Set<List<TaggedWord>> words =  new HashSet<>(l1._3());
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
            List<TaggedWord> representation = t._1().stream().map((s) -> s.words.get(0)).collect(Collectors.toList());
            System.out.println(representation.toString() + ": " + t._2().toString() + " | " + t._3().toString());
            System.out.println("Feature: " + template.getFeature(representation));
        });
        System.out.println(clusters.size());

        JavaRDD<Tuple3<List<VectorWithWords>, Integer, Set<List<TaggedWord>>>> clusterRDD = context.parallelize(clusters);
        clusterRDD.cache();
        JavaPairRDD<String, Integer> wordsForLinModelRDD = clusterRDD.flatMapToPair((Tuple3<List<VectorWithWords>, Integer, Set<List<TaggedWord>>> cluster) -> {
            List<Tuple2<String, Integer>> words = new ArrayList<Tuple2<String, Integer>>();
            cluster._3().forEach((List<TaggedWord> wordList) -> {
                wordList.forEach((TaggedWord word) -> {
                    words.add(new Tuple2<String, Integer>(word.word(), 1));
                });
            });
            return words;
        }).reduceByKey((a,b) -> a+b).filter((Tuple2<String, Integer> tuple) -> tuple._2()>10);

        /*LinkedList<String> features = new LinkedList<>();
        clusters.stream().forEach((t) -> {
            List<TaggedWord> representation = t._1().stream().map((s) -> s.words.get(0)).collect(Collectors.toList());
            features.add(template.getFeature(representation));
        });*/

        Set<String> features1 = new HashSet<>();
        Set<String> wordsForLinModel = wordsForLinModelRDD.collectAsMap().keySet();
        String[] wordsForLinModelArray = wordsForLinModel.toArray(new String[wordsForLinModel.size()]);


        //List<String> features = new ArrayList<>(features1);
        Broadcast<String[]> featureBroadcast = context.broadcast(wordsForLinModelArray);
        //Broadcast<Map<List<TaggedWord>, String>> featureMapBroadcast = context.broadcast(featureMap);

        JavaRDD points = tagRDD.map(rating -> {
            String[] fbc = featureBroadcast.getValue();
            //Map<List<TaggedWord>, String> fmp = featureMapBroadcast.getValue();
            double[] v = new double[fbc.length];
            List<Tuple2<List<TaggedWord>, Integer>> output =  BigramThesis.findKGramsEx(3, rating._1, template);
            List<String> foundWords = new LinkedList<String>();
            output.forEach((Tuple2<List<TaggedWord>, Integer> tuple)->{
                tuple._1().forEach((TaggedWord tword)->{
                    foundWords.add(tword.word());
                });
            });
            for(int i=0; i< fbc.length; i++){
                int index = i;
                v[i] = foundWords.stream().filter((String word1) -> word1.equals(fbc[index])).count();
            }
            return new LabeledPoint((double) (rating._2), Vectors.dense(v).toSparse());
        });

        DataFrame training = sqlContext.createDataFrame(points, LabeledPoint.class);


        org.apache.spark.ml.regression.LinearRegression lr = new org.apache.spark.ml.regression.LinearRegression();

        lr.setMaxIter(10)
                .setRegParam(0.01);

        LinearRegressionModel model1 = lr.train(training);

        System.out.println("Model 1 was fit using parameters: " + model1.coefficients());

        Map<List<TaggedWord>, String> featureMap= new HashMap<>();
        clusters.forEach((cluster) -> {
                    List<TaggedWord> representation = cluster._1().stream().map((s) -> s.words.get(0)).collect(Collectors.toList());
                    String feature = template.getFeature(representation);
                    if (wordsForLinModel.contains(feature)) {
                        features1.add(feature);
                        cluster._3().forEach(listOfTaggedWords -> {
                            featureMap.put(listOfTaggedWords, feature);
                        });
                    }

                }
        );

        Map<String, Set<String>> featureToWordsMap = new HashMap<>();
        for (String feature : featureMap.values()){
            Set<String> asd = new HashSet<>();
            featureMap.entrySet().stream().filter((Map.Entry<List<TaggedWord>, String> entry) ->{
                return entry.getValue().equals(feature);
            }).forEach((Map.Entry<List<TaggedWord>, String> entry2) ->{
                Set<String> words = new HashSet<String>();
                entry2.getKey().forEach((TaggedWord tword) -> {
                    asd.add(tword.word());
                });
            });
            featureToWordsMap.put(feature, asd);
        }
        double[] coeffs = model1.coefficients().toArray();
        Map<String, List<Tuple2<String, Double>>> featureToWordMapWithCoeffs = new HashMap<>();
        List<String> wordsForLinModelArrayAsList =  Arrays.asList(wordsForLinModelArray);
        for (Map.Entry<String, Set<String>> entry : featureToWordsMap.entrySet()){
            List<Tuple2<String, Double>> g = new LinkedList<>();
            for (String s : entry.getValue()) {
                Integer index = wordsForLinModelArrayAsList.indexOf(s);
                if (index >=0){
                    g.add(new Tuple2<String, Double>(s, coeffs[index]));
                }
            }
            featureToWordMapWithCoeffs.put(entry.getKey(), g);

        }
        for (Map.Entry<String, List<Tuple2<String, Double>>> entry : featureToWordMapWithCoeffs.entrySet()){
            System.out.println(entry.getKey());
            System.out.print("\tmost popular Words: ");
            entry.getValue().stream().sorted((t1, t2) -> t2._2().compareTo(t1._2())).limit(3).forEach(System.out::print);
            System.out.println();
            System.out.print("\tleast popular Words: ");
            entry.getValue().stream().sorted((t1, t2) -> t1._2().compareTo(t2._2())).limit(3).forEach(System.out::print);
            System.out.println();
        }

        /*for (int i = 0; i < coeffs.length; i++) {
            map.put(wordsForLinModelArray[i], coeffs[i]);

        }

        Iterator<Map.Entry<String, Double>> i = Utility.valueIterator(map);
        System.out.println("Positive Words");
        int j = 0;
        while (j < 50) {
            Map.Entry<String, Double> entry = i.next();
            System.out.println(entry);
            j++;
        }
        System.out.println("");
        System.out.println("Negative Words");
        i = Utility.valueIteratorReverse(map);
        j = 0;
        while (j < 50) {
            Map.Entry<String, Double> entry = i.next();
            System.out.println(entry);
            j++;

        }*/

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
        public List<TaggedWord> words;
        public Vector vector;
        public Template template;
        public String representative;

        public VectorWithWords(final Vector vector, final List<TaggedWord> words, final Template template) {
            this.words = words;
            this.template = template;
            this.vector = vector;
            this.representative = this.template.getFeature(this.words);
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
