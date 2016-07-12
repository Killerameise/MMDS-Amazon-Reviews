package de.hpi.mmds;

import de.hpi.mmds.database.MetadataRecord;
import de.hpi.mmds.database.ReviewRecord;
import de.hpi.mmds.filter.CategoryFilter;
import de.hpi.mmds.filter.PriceFilter;
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
    private static String reviewPath = "resources/reviews";
    private static int CPUS = 8;

    public static void main(String args[]) {

        SparkConf conf = new SparkConf();
        conf.setIfMissing("spark.master", "local[" + CPUS + "]");
        conf.setAppName("mmds-amazon");
        JavaSparkContext context = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);

        if (args.length >= 1) {
            reviewPath = args[0];
        } else {
            File folder = new File(reviewPath);
            File[] reviewFiles = folder.listFiles((dir, name) -> name.endsWith(".json"));
            reviewPath = reviewFiles[0].getAbsolutePath();
        }

        if (args.length >= 2) {
            CPUS = Integer.valueOf(args[1]);
        }

        List<String> products = null;
        if (args.length >= 3) {
            String metadataFile = args[2];
            JavaRDD<MetadataRecord> inputRDD = context.textFile(metadataFile, CPUS).map(JsonReader::readMetadataJson);
            PriceFilter filter = new PriceFilter(new CategoryFilter(inputRDD, "Guitar & Bass Accessories").chain(), 5.0, 500.0);
            products = filter.toList();
        }
        Broadcast<List<String>> productBroadcast = context.broadcast(products);

        JavaRDD<String> fileRDD = context.textFile(reviewPath, CPUS);

        JavaRDD<ReviewRecord> recordsRDD = fileRDD.map(JsonReader::readReviewJson);

        if (productBroadcast.getValue() != null) {
            recordsRDD = recordsRDD.filter((record) -> productBroadcast.getValue().contains(record.asin));
        }

        JavaPairRDD<List<TaggedWord>, Float> tagRDD = recordsRDD.mapToPair(
                (ReviewRecord x) -> new Tuple2(Utility.posTag(x.getReviewText()), x.getOverall()));

        tagRDD.cache();

        JavaRDD<List<String>> textRdd = tagRDD.map(
                a -> a._1.stream().map(Word::word).collect(Collectors.toList())
        );

        // Learn a mapping from words to Vectors.
        Word2Vec word2Vec = new Word2Vec()
                .setVectorSize(50)
                .setMinCount(0)
                .setNumPartitions(CPUS);
        Word2VecModel model = word2Vec.fit(textRdd);


        Template template = new AdjectiveNounTemplate(); //TODO: use more templates again
        JavaRDD<List<Tuple2<List<TaggedWord>, Integer>>> rddValuesRDD = tagRDD.map(
                taggedWords -> BigramThesis.findKGramsEx(3, taggedWords._1(), template)
        ); //TODO: carry on review id and review score for use by linear model

        JavaPairRDD<List<TaggedWord>, Integer> semiFinalRDD = rddValuesRDD.flatMapToPair(a -> a).reduceByKey(
                (a, b) -> a + b);

        JavaPairRDD<Match, Integer> vectorRDD = semiFinalRDD.mapToPair(a -> {
            List<VectorWithWords> vectors = a._1().stream().map(
                    (TaggedWord taggedWord) ->
                            new VectorWithWords(model.transform(taggedWord.word()), taggedWord))
                    .collect(Collectors.toList());
            return new Tuple2<>(new Match(vectors, template), a._2);
        });

        JavaPairRDD<Match, Integer> repartitionedVectorRDD = vectorRDD.repartition(CPUS);

        List<MergedVector> clusters = repartitionedVectorRDD.treeAggregate(
                new LinkedList<>(),
                (List<MergedVector> acc, Tuple2<Match, Integer> value) -> {
                    Boolean foundOne = false;
                    List<MergedVector> new_acc = new LinkedList<>(acc);
                    for (int i = 0; i < acc.size(); i++) {
                        MergedVector l = acc.get(i);
                        if(l.feature.equals(value._1().representative) || compare(value._1(), l)){
                            new_acc.remove(i);
                            Set<NGramm> words = new HashSet<>(l.ngrams);
                            words.add(value._1().ngram);
                            new_acc.add(new MergedVector(l.vector, l.template, words, l.count + value._2()));
                            foundOne = true;
                            break;
                        }
                    }
                    if (!foundOne) {
                        Set<NGramm> words = new HashSet<>();
                        words.add(value._1().ngram);
                        new_acc.add(new MergedVector(value._1().vectors, value._1().template, words, value._2()));
                    }
                    return new_acc;

                },
                (List<MergedVector> acc1, List<MergedVector> acc2) -> {
                    List<MergedVector> dotProduct = new LinkedList<>();
                    List<MergedVector> result = new LinkedList<>();
                    Set<Integer> deletedItems = new HashSet<>();
                    dotProduct.addAll(acc1);
                    dotProduct.addAll(acc2);
                    for (int i = 0; i < dotProduct.size(); i++) {
                        MergedVector l1 = dotProduct.get(i);
                        for (int j = i + 1; j < dotProduct.size(); j++) {
                            if (deletedItems.contains(j)) continue;
                            MergedVector l2 = dotProduct.get(j);
                            if(l1.feature.equals(l2.feature) || compare(l1, l2)){
                                Set<NGramm> words = new HashSet<>(l1.ngrams);
                                words.addAll(l2.ngrams);
                                l1 = new MergedVector(l1.vector, l1.template, words, l1.count + l2.count);
                                deletedItems.add(j);
                            }
                        }
                        result.add(l1);
                    }
                    return result;
                }
        );

        System.out.println(clusters.size());

        JavaRDD<MergedVector> mergedVectorRDD = context.parallelize(clusters, CPUS);
        JavaPairRDD<MergedVector, Integer> unsortedClustersRDD = mergedVectorRDD.mapToPair(
                (t) -> new Tuple2<>(t, t.count));

        JavaPairRDD<MergedVector, Integer> sortedClustersRDD = unsortedClustersRDD.mapToPair(Tuple2::swap)
                .sortByKey(false).mapToPair(Tuple2::swap);

        JavaRDD<MergedVector> finalClusterRDD = sortedClustersRDD.map(Tuple2::_1);

        finalClusterRDD.take(25).forEach((t) -> {
            List<TaggedWord> representation = t.getNGramm().taggedWords;
            System.out.println(representation.toString() + ": " + t.count.toString() + " | " + t.ngrams.stream()
                    .map(n -> n.taggedWords.stream().map(tw -> tw.word()).collect(Collectors.joining(", ")))
                    .collect(Collectors.joining(" + ")));
            System.out.println("Feature: " + template.getFeature(representation));
        });

        Set<String> features1 = new HashSet<>();
        Set<String> descriptions1 = new HashSet<>();
        //Map<List<TaggedWord>, String> featureMap = new HashMap<>();

        List<Tuple2<Tuple2<String, String>, Double>> weighted = new LinkedList<>();
        finalClusterRDD.take(25).forEach((MergedVector cluster) -> {
            String feature = cluster.feature;
            features1.add(feature);
            List<String> descriptions = new ArrayList<String>(cluster.descriptions);
            Map<List<TaggedWord>, String> featureMap = new HashMap<>();
            cluster.ngrams.forEach((NGramm listOfTaggedWords) -> {
                featureMap.put(listOfTaggedWords.taggedWords, feature);
            });
            JavaRDD<LabeledPoint> points = tagRDD.map((Tuple2<List<TaggedWord>, Float> rating) -> {
                //Map<List<TaggedWord>, String> fmp = descriptionMapBroadcast.getValue();
                double[] v = new double[descriptions.size()];
                List<NGramm> output = new LinkedList<NGramm>();
                BigramThesis.findKGramsEx(3, rating._1, template).forEach(result ->
                        output.add(new NGramm(result._1(), template)));
                //List<TaggedWord> output = rating._1().stream().filter((TaggedWord tw) -> (fbc.contains(tw.word()))).collect(Collectors.toList());
                Boolean foundOne = false;
                for (NGramm ngram : output) {
                    String description = ngram.template.getDescription(ngram.taggedWords);
                    foundOne = true;
                    int index = descriptions.indexOf(description);
                    if (index >= 0) {
                        v[index] = 1;
                    }
                }
                if (foundOne) {
                    return new LabeledPoint((double) (rating._2), Vectors.dense(v));
                } else return null;
            }).filter(point -> point != null);

            DataFrame training = sqlContext.createDataFrame(points, LabeledPoint.class);

            org.apache.spark.ml.regression.LinearRegression lr = new org.apache.spark.ml.regression.LinearRegression();

            lr.setMaxIter(20)
                    .setRegParam(0.05);

            LinearRegressionModel model1 = lr.train(training);

            System.out.println("Model 1 was fit using parameters: " + model1.coefficients());

            List<Tuple2<Tuple2<String, String>, Double>> list = new LinkedList<>();
            double[] coeffs = model1.coefficients().toArray();
            for (int i = 0; i < coeffs.length; i++) {
                int index = i;
                list.add(new Tuple2<>(new Tuple2<String, String>(descriptions.get(i), feature), coeffs[i]));
            }
            weighted.addAll(list);
//            Iterator<Map.Entry<String, Double>> i = Utility.valueIterator(map);
//            Set<String> features3 = new HashSet<>(featureMap.values());
//            for (String feature3 : features3) {
//                Map<String, Double> scores = new HashMap<>();
//                for (Map.Entry<List<TaggedWord>, String> entry : featureMap.entrySet()) {
//                    if (entry.getValue().equals(feature3)) {
//                        for (TaggedWord word : entry.getKey()) {
//                            if (map.containsKey(word.word())) {
//                                scores.put(word.word(), map.get(word.word()));
//                            }
//                        }
//                    }
//                }
//                System.out.println(feature3 + ": ");
//                System.out.println(scores);
//            }

        });

        JavaPairRDD<Tuple2<String, String>, Double> weighted2 = context.parallelizePairs(weighted);
        System.out.println("Most positive");

        weighted2.mapToPair(Tuple2::swap).sortByKey(false).take(25).forEach(tuple -> System.out.println(tuple._2() + ": " + tuple._1()));
        System.out.println("Most negative");
        weighted2.mapToPair(Tuple2::swap).sortByKey(true).take(25).forEach(tuple -> System.out.println(tuple._2() + ": " + tuple._1()));

        //List<String> descriptions = new ArrayList<>(descriptions1);
        //Broadcast<List<String>> descriptionBroadcast = context.broadcast(descriptions);
        //Broadcast<Map<List<TaggedWord>, String>> descriptionMapBroadcast = context.broadcast(featureMap);




            /*featureMap.entrySet().stream().filter((Map.Entry<List<TaggedWord>, String> entry) -> (features.indexOf(entry.getValue()) == index))
                    .forEach((Map.Entry<List<TaggedWord>, String> entry2) -> {
                        map.put(entry2.getValue(), coeffs[index]);
                    });*/ // a beauty of its own quality







        /*System.out.println("Positive Words");
        int j = 0;
        while (j < 50 && i.hasNext()) {
            Map.Entry<String, Double> entry = i.next();
            System.out.println(entry);
            j++;
        }
        System.out.println("");
        System.out.println("Negative Words");
        i = Utility.valueIteratorReverse(map);
        j = 0;
        while (j < 50 && i.hasNext() ) {
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

    public static boolean compare(TemplateBased t1, TemplateBased t2) {
        double threshold = 0.9;

        double[] v1 = null;
        String s1 = t1.getTemplate().getFeature(t1.getNGramm().taggedWords);
        for (VectorWithWords v : t1.getVectors()) {
            if (v.word.word().equals(s1)) {
                v1 = v.vector.toArray();
            }
        }

        double[] v2 = null;
        String s2 = t2.getTemplate().getFeature(t2.getNGramm().taggedWords);
        for (VectorWithWords v : t2.getVectors()) {
            if (v.word.word().equals(s2)) {
                v2 = v.vector.toArray();
            }
        }

        if (v1 == null || v2 == null) {
            return false;
        }
        return threshold < cosineSimilarity(v1, v2);
    }

    public static class VectorWithWords implements Serializable {
        public TaggedWord word;
        public Vector     vector;

        public VectorWithWords(final Vector vector, final TaggedWord words) {
            this.word = words;
            this.vector = vector;
        }

        @Override
        public String toString() {
            return "VectorWithWords{" +
                   ", vector=" + vector +
                   '}';
        }
    }

    interface TemplateBased {
        Template getTemplate();

        NGramm getNGramm();

        List<VectorWithWords> getVectors();
    }

    public static class Match implements Serializable, TemplateBased {
        public List<VectorWithWords> vectors;
        public NGramm                ngram;
        public Template              template;
        public String                representative;

        public Match(final List<VectorWithWords> vlist, final Template template) {
            this.vectors = vlist;
            this.template = template;
            List<TaggedWord> words = new LinkedList<>();
            this.vectors.forEach(a -> words.add(a.word));
            this.ngram = new NGramm(words, template);
            this.representative = template.getFeature(words);
        }

        @Override
        public Template getTemplate() {
            return template;
        }

        @Override
        public NGramm getNGramm() {
            return ngram;
        }

        @Override
        public List<VectorWithWords> getVectors() {
            return vectors;
        }
    }

    public static class MergedVector implements Serializable, TemplateBased {
        public List<VectorWithWords> vector;
        public Template              template;
        public String                feature;
        public Set<String>           descriptions;
        public Set<NGramm>           ngrams;
        public NGramm                representative;
        public Integer               count;

        public MergedVector(final List<VectorWithWords> vector,
                            final Template template,
                            final Set<NGramm> ngrams,
                            final Integer count) {
            this.vector = vector;
            this.template = template;
            this.ngrams = ngrams;
            this.count = count;
            List<TaggedWord> words = new LinkedList<>();
            this.vector.forEach(a -> words.add(a.word));
            this.feature = template.getFeature(words);
            this.descriptions = new HashSet<>();
            for (NGramm ngram : this.ngrams) {
                this.descriptions.add(ngram.template.getDescription(ngram.taggedWords));
            }
            this.representative = this.ngrams.iterator().next();
        }

        @Override
        public Template getTemplate() {
            return template;
        }

        @Override
        public NGramm getNGramm() {
            return representative;
        }

        @Override
        public List<VectorWithWords> getVectors() {
            return vector;
        }
    }

    public static class NGramm implements Serializable {
        public List<TaggedWord> taggedWords;
        public Template         template;

        public NGramm(List<TaggedWord> twords, Template template) {
            this.taggedWords = twords;
            this.template = template;
        }
    }
}
