package de.hpi.mmds.clustering;

import de.hpi.mmds.nlp.Match;
import de.hpi.mmds.nlp.MergedVector;
import de.hpi.mmds.nlp.NGram;
import de.hpi.mmds.nlp.VectorWithWords;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class DIMSUM implements NGramClustering, Serializable {

    public JavaRDD<MergedVector> resolveDuplicates(JavaPairRDD<Match, Integer> repartitionedVectorRDD,
                                                   Double threshold, JavaSparkContext context, Integer CPUS) {
        JavaPairRDD<Tuple2<Match, Integer>, Long> repartitionedVectorRDD2 = repartitionedVectorRDD.zipWithIndex();
        repartitionedVectorRDD2.cache();
        JavaPairRDD<Long, Tuple2<Match, Integer>> swappedRepartitionedVectorRDD =
                repartitionedVectorRDD2.mapToPair(Tuple2::swap);

        JavaPairRDD<Vector, Long> indexedVectors = repartitionedVectorRDD2.mapToPair(
                (Tuple2<Tuple2<Match, Integer>, Long> tuple) -> {
                    Match match = tuple._1()._1();
                    String feature = match.getTemplate().getFeature(match.getNGramm().taggedWords);
                    Vector vector = null;
                    for (VectorWithWords v : match.getVectors()) {
                        if (v.word.word().equals(feature)) {
                            vector = v.vector;
                        }
                    }
                    if (vector == null) vector = match.getVectors().get(0).vector;
                    return new Tuple2<>(vector, tuple._2());
                });
        JavaRDD<IndexedRow> rows = indexedVectors.map(tuple -> new IndexedRow(tuple._2(), tuple._1()));

        IndexedRowMatrix mat = new IndexedRowMatrix(rows.rdd());

        System.out.println("Transposing Matrix");

        RowMatrix mat2 = transpose.transposeRowMatrix(mat);

        System.out.println("computing similarities");

        CoordinateMatrix coords = mat2.columnSimilarities(0.3);
        JavaRDD<MatrixEntry> entries = coords.entries().toJavaRDD();
        System.out.println("finished");

        JavaPairRDD<Long, Long> asd = graphops.getConnectedComponents(entries.filter(
                (matrixEntry) -> matrixEntry.value() > threshold).rdd()).mapToPair(
                (Tuple2<Object, Object> tuple) -> new Tuple2<>(Long.parseLong(tuple._1().toString()),
                                                               Long.parseLong(tuple._2().toString())));

        JavaPairRDD<Long, Match> h = asd.join(swappedRepartitionedVectorRDD).mapToPair(
                (tuple) -> new Tuple2<>(tuple._2()._1(), tuple._2()._2()._1()));

        JavaPairRDD<Long, Iterable<Match>> i2 = h.groupByKey();

        return i2.map(value -> {
            Set<NGram> ngrams = new HashSet<>();
            value._2().iterator().forEachRemaining(it -> ngrams.add(it.getNGramm()));
            Match mv = value._2().iterator().next();
            return new MergedVector(mv.getVectors(), mv.template, ngrams, ngrams.size());
        });

    }
}
