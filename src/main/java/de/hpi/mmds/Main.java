package de.hpi.mmds;

import de.hpi.mmds.clustering.AggregateDupDet;
import de.hpi.mmds.clustering.DIMSUM;
import de.hpi.mmds.clustering.ExactClustering;
import de.hpi.mmds.clustering.NGramClustering;
import de.hpi.mmds.database.MetadataRecord;
import de.hpi.mmds.database.ReviewRecord;
import de.hpi.mmds.filter.CategoryFilter;
import de.hpi.mmds.filter.PriceFilter;
import de.hpi.mmds.json.JsonReader;
import de.hpi.mmds.nlp.*;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;


public class Main {
    private static final double THRESHOLD = 0.95;

    private static String reviewPath = "resources/reviews";
    private static int CPUS = 8;

    public static void main(String args[]) {

        reviewPath = handleReviewPathParameter(args);
        CPUS = handleWorkerParameter(args);

        SparkConf conf = new SparkConf();
        conf.setIfMissing("spark.master", "local[" + CPUS + "]");
        conf.setAppName("mmds-amazon " + Arrays.asList(args));
        JavaSparkContext context = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);

        List<String> products = handleFilterParameter(args, context);
        Broadcast<List<String>> productBroadcast = context.broadcast(products);

        NGramClustering clusterAlgorithm = handleClusteringAlgorithmParameter(args);
        boolean useWord2Vec = clusterAlgorithm.getClass() != ExactClustering.class;

        JavaRDD<String> fileRDD = context.textFile(reviewPath, CPUS);

        JavaRDD<ReviewRecord> recordsRDD = fileRDD.map(JsonReader::readReviewJson);

        if (productBroadcast.getValue() != null) {
            recordsRDD = recordsRDD.filter((record) -> productBroadcast.getValue().contains(record.asin));
        }

        JavaPairRDD<List<TaggedWord>, Float> tagRDD = recordsRDD.mapToPair(
                (ReviewRecord x) -> new Tuple2<>(Utility.posTag(x.getReviewText()), x.getOverall()));

        tagRDD.cache();

        JavaRDD<List<String>> textRdd = tagRDD.map(
                a -> a._1.stream().map(Word::word).collect(Collectors.toList())
        );

        Word2VecModel word2VecModel = getWord2VecModel(useWord2Vec, textRdd);
        Broadcast<Boolean> word2vecBroadcast = context.broadcast(useWord2Vec);
        Broadcast<Word2VecModel> modelBroadcast = context.broadcast(word2VecModel);

        Template template = new AdjectiveNounTemplate();
        JavaRDD<List<Tuple2<List<TaggedWord>, Integer>>> rddValuesRDD = tagRDD.map(
                taggedWords -> BigramThesis.findNGrams(3, taggedWords._1(), template)
        );

        JavaPairRDD<List<TaggedWord>, Integer> semiFinalRDD = rddValuesRDD.flatMapToPair(a -> a).reduceByKey(
                (a, b) -> a + b);

        JavaPairRDD<Match, Integer> vectorRDD = semiFinalRDD.mapToPair(a -> {
            List<VectorWithWords> vectors = a._1().stream().map(
                    (TaggedWord taggedWord) -> {
                        final boolean word2vec = word2vecBroadcast.getValue();
                        Vector vector = null;
                        if (word2vec) {
                            final Word2VecModel model = modelBroadcast.getValue();
                            vector = model.transform(taggedWord.word());
                        }
                        return new VectorWithWords(vector, taggedWord);
                    }).collect(Collectors.toList());
            return new Tuple2<>(new Match(vectors, template), a._2);
        });

        JavaPairRDD<Match, Integer> repartitionedVectorRDD = vectorRDD.repartition(CPUS);

        JavaPairRDD<MergedVector, Integer> unsortedClustersRDD = clusterAlgorithm.resolveDuplicates(
                repartitionedVectorRDD, THRESHOLD, context, CPUS).mapToPair((t) -> new Tuple2<>(t, t.count));

        JavaPairRDD<MergedVector, Integer> sortedClustersRDD = unsortedClustersRDD.mapToPair(Tuple2::swap)
                .sortByKey(false).mapToPair(Tuple2::swap);

        JavaRDD<MergedVector> finalClusterRDD = sortedClustersRDD.map(Tuple2::_1);

        finalClusterRDD.take(25).forEach((t) -> {
            List<TaggedWord> representation = t.getNGramm().taggedWords;
            System.out.println(representation.toString() + ": " + t.count.toString() + " | " + t.ngrams.stream()
                    .map(n -> n.taggedWords.stream().map(Word::word).collect(Collectors.joining(", ")))
                    .collect(Collectors.joining(" + ")));
            System.out.println("Feature: " + template.getFeature(representation));
        });

        List<Tuple2<Tuple2<String, String>, Double>> results = buildLinearModels(sqlContext, tagRDD, finalClusterRDD);
        JavaPairRDD<Double, Tuple2<String, String>> weighted = context.parallelizePairs(results).mapToPair(Tuple2::swap);

        List<Tuple2<Double, Tuple2<String, String>>> mostPositive = weighted.sortByKey(false).take(25);
        List<Tuple2<Double, Tuple2<String, String>>> mostNegative = weighted.sortByKey(true).take(25);

        System.out.println("Most positive");
        mostPositive.forEach(tuple -> System.out.println(tuple._2() + ": " + tuple._1()));
        System.out.println("Most negative");
        mostNegative.forEach(tuple -> System.out.println(tuple._2() + ": " + tuple._1()));
    }

    private static String handleReviewPathParameter(String[] args) {
        if (args.length >= 1) {
            return args[0];
        } else {
            File folder = new File(reviewPath);
            File[] reviewFiles = folder.listFiles((dir, name) -> name.endsWith(".json"));
            return reviewFiles[0].getAbsolutePath();
        }
    }

    private static int handleWorkerParameter(String[] args) {
        if (args.length >= 2) {
            return Integer.valueOf(args[1]);
        } else {
            return CPUS;
        }
    }

    private static List<String> handleFilterParameter(String[] args, JavaSparkContext context) {
        if (args.length >= 3) {
            String metadataFile = args[2];
            JavaRDD<MetadataRecord> inputRDD = context.textFile(metadataFile, CPUS).map(JsonReader::readMetadataJson);
            PriceFilter filter = new PriceFilter(new CategoryFilter(inputRDD, "Guitar & Bass Accessories").chain(),
                                                 5.0, 500.0);
            return filter.toList();
        }
        return null;
    }

    private static NGramClustering handleClusteringAlgorithmParameter(String[] args) {
        if (args.length >= 4) {
            String algorithm = args[3];
            if (algorithm.equals("DIMSUM")) {
                return new DIMSUM();
            } else if (algorithm.equals("TreeAggregate")) {
                return new AggregateDupDet();
            }
        }
        return new ExactClustering();
    }

    private static Word2VecModel getWord2VecModel(boolean useWord2Vec, JavaRDD<List<String>> textRdd) {
        if (useWord2Vec) {
            Word2Vec word2Vec = new Word2Vec()
                    .setVectorSize(50)
                    .setMinCount(0)
                    .setNumPartitions(CPUS);
            return word2Vec.fit(textRdd);
        }
        return null;
    }

    private static List<Tuple2<Tuple2<String, String>, Double>> buildLinearModels(
            SQLContext sqlContext, JavaPairRDD<List<TaggedWord>, Float> tagRDD, JavaRDD<MergedVector> finalClusterRDD) {
        List<Tuple2<Tuple2<String, String>, Double>> weighted = new LinkedList<>();
        finalClusterRDD.take(25).forEach((MergedVector cluster) -> {
            String feature = cluster.feature;
            List<String> descriptions = new ArrayList<>(cluster.descriptions);
            JavaRDD<LabeledPoint> points = tagRDD.map((Tuple2<List<TaggedWord>, Float> rating) -> {
                double[] v = new double[descriptions.size()];
                List<NGram> output = new LinkedList<>();
                BigramThesis.findNGrams(3, rating._1, cluster.template).forEach(result ->
                        output.add(new NGram(result._1(), cluster.template)));
                Boolean foundOne = false;
                for (NGram ngram : output) {
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
            double[] coefficients = model1.coefficients().toArray();
            for (int i = 0; i < coefficients.length; i++) {
                list.add(new Tuple2<>(new Tuple2<>(descriptions.get(i), feature), coefficients[i]));
            }
            weighted.addAll(list);
        });
        return weighted;
    }

}
