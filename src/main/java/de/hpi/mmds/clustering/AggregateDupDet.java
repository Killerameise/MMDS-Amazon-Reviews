package de.hpi.mmds.clustering;

import de.hpi.mmds.nlp.Match;
import de.hpi.mmds.nlp.MergedVector;
import de.hpi.mmds.nlp.NGram;
import de.hpi.mmds.nlp.VectorWithWords;
import de.hpi.mmds.nlp.template.TemplateBased;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class AggregateDupDet implements NGramClustering {
    public JavaRDD<MergedVector> resolveDuplicates(JavaPairRDD<Match, Integer> repartitionedVectorRDD,
                                                   Double threshold, JavaSparkContext context, Integer CPUS) {
        List<MergedVector> clusters = repartitionedVectorRDD.treeAggregate(
                new LinkedList<>(),
                (List<MergedVector> acc, Tuple2<Match, Integer> value) -> {
                    Boolean foundOne = false;
                    List<MergedVector> new_acc = new LinkedList<>(acc);
                    for (int i = 0; i < acc.size(); i++) {
                        MergedVector l = acc.get(i);
                        if (l.feature.equals(value._1().representative) || compare(value._1(), l)) {
                            new_acc.remove(i);
                            Set<NGram> words = new HashSet<>(l.ngrams);
                            words.add(value._1().ngram);
                            new_acc.add(new MergedVector(l.vector, l.template, words, l.count + value._2()));
                            foundOne = true;
                            break;
                        }
                    }
                    if (!foundOne) {
                        Set<NGram> words = new HashSet<>();
                        words.add(value._1().ngram);
                        new_acc.add(new MergedVector(value._1().vectors, value._1().template, words, value._2()));
                    }
                    return new_acc;

                },
                (List<MergedVector> acc1, List<MergedVector> acc2) -> {
                    List<MergedVector> dotProduct = new LinkedList<>();
                    List<MergedVector> result = new LinkedList<>();
                    dotProduct.addAll(acc1);
                    dotProduct.addAll(acc2);
                    for (int i = 0; i < dotProduct.size(); i++) {
                        Boolean foundOne = false;
                        MergedVector l1 = dotProduct.get(i);
                        for (int j = i + 1; j < dotProduct.size(); j++) {
                            MergedVector l2 = dotProduct.get(j);
                            if (l1.feature.equals(l2.feature) || compare(l1, l2)) {
                                Set<NGram> words = new HashSet<>(l1.ngrams);
                                words.addAll(l2.ngrams);
                                result.add(new MergedVector(l1.vector, l1.template, words, l1.count + l2.count));
                                foundOne = true;
                                break;
                            }
                        }
                        if (!foundOne) {
                            result.add(new MergedVector(l1.vector, l1.template, l1.ngrams, l1.count));
                        }
                    }
                    return result;
                }
        );
        return context.parallelize(clusters, CPUS);

    }

    private static double cosineSimilarity(double[] docVector1, double[] docVector2) {
        double dotProduct = 0.0;
        double magnitude1 = 0.0;
        double magnitude2 = 0.0;
        double cosineSimilarity = 0.0;

        assert(docVector1.length == docVector2.length);
        for (int i = 0; i < docVector1.length; i++)
        {
            dotProduct += docVector1[i] * docVector2[i];
            magnitude1 += Math.pow(docVector1[i], 2);
            magnitude2 += Math.pow(docVector2[i], 2);
        }

        magnitude1 = Math.sqrt(magnitude1);
        magnitude2 = Math.sqrt(magnitude2);

        if (magnitude1 != 0.0 | magnitude2 != 0.0) {
            cosineSimilarity = dotProduct / (magnitude1 * magnitude2);
        }
        return cosineSimilarity;
    }

    private static boolean compare(TemplateBased t1, TemplateBased t2) {
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
}
