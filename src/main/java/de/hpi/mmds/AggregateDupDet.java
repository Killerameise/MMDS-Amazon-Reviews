package de.hpi.mmds;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import de.hpi.mmds.Main.*;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static de.hpi.mmds.Main.compare;

/**
 * Created by axel on 12.07.16.
 */
public class AggregateDupDet {
    public static JavaRDD<Main.MergedVector> resolveDuplicates(JavaPairRDD<Main.Match, Integer> repartitionedVectorRDD, Double threshold, JavaSparkContext context, Integer CPUS) {
        List<MergedVector> clusters = repartitionedVectorRDD.treeAggregate(
                new LinkedList<>(),
                (List<MergedVector> acc, Tuple2<Match, Integer> value) -> {
                    Boolean foundOne = false;
                    List<MergedVector> new_acc = new LinkedList<>(acc);
                    for (int i = 0; i < acc.size(); i++) {
                        MergedVector l = acc.get(i);
                        if (l.feature.equals(value._1().representative) || compare(value._1(), l)) {
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
                    dotProduct.addAll(acc1);
                    dotProduct.addAll(acc2);
                    for (int i = 0; i < dotProduct.size(); i++) {
                        Boolean foundOne = false;
                        MergedVector l1 = dotProduct.get(i);
                        for (int j = i + 1; j < dotProduct.size(); j++) {
                            MergedVector l2 = dotProduct.get(j);
                            if (l1.feature.equals(l2.feature) || compare(l1, l2)) {
                                Set<NGramm> words = new HashSet<>(l1.ngrams);
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

        System.out.println(clusters.size());

        JavaRDD<MergedVector> mergedVectorRDD = context.parallelize(clusters, CPUS);

        return mergedVectorRDD;

    }
}
