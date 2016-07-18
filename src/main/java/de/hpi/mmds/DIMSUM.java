package de.hpi.mmds;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.*;
import de.hpi.mmds.Main.*;
import de.hpi.mmds.transpose;
import scala.Tuple2;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


/**
 * Created by axel on 12.07.16.
 */
public class DIMSUM{

    public static JavaRDD<Main.MergedVector> resolveDuplicates(JavaPairRDD<Main.Match, Integer> repartitionedVectorRDD, Double threshold, JavaSparkContext context, Integer CPUS) {
        JavaPairRDD<Tuple2<Match, Integer>, Long> repartitionedVectorRDD2 = repartitionedVectorRDD.zipWithIndex();
        repartitionedVectorRDD2.cache();
        JavaPairRDD<Long, Tuple2<Main.Match, Integer>> swappedRepartitionedVectorRDD = repartitionedVectorRDD2.mapToPair(Tuple2::swap);

        //repartitionedVectorRDD.cache();
        JavaPairRDD<Vector, Long> indexedVectors = repartitionedVectorRDD2.mapToPair((Tuple2<Tuple2<Main.Match, Integer>, Long> tuple) -> (new Tuple2<Vector, Long>(tuple._1()._1().getVectors().get(0).vector, tuple._2())));


        System.out.println("Transposing Matrix");
        RowMatrix mat2 = computeRowmatrixViaCoordMat(indexedVectors);
        //RowMatrix mat2 = computeRowmatrixViaTranspose(indexedVectors);

        System.out.println("computing similarities");

        CoordinateMatrix coords = mat2.columnSimilarities(0.3);
        JavaRDD<MatrixEntry> entries = coords.entries().toJavaRDD();
        System.out.println("finished");

        JavaPairRDD<Long, Long> asd = graphops.getConnectedComponents(entries.filter(matrixEntry -> matrixEntry.value() > threshold).rdd()).
                mapToPair((Tuple2<Object, Object> tuple) -> new Tuple2<Long, Long>(Long.parseLong(tuple._1().toString()), Long.parseLong(tuple._2().toString())));

        JavaPairRDD<Long, Main.Match> h = asd.join(swappedRepartitionedVectorRDD).mapToPair(tuple -> new Tuple2<Long, Main.Match>(tuple._2()._1(), tuple._2()._2()._1()));

        JavaPairRDD<Long, Iterable<Match>> i2 = h.groupByKey();

        JavaRDD<MergedVector> mergedVectorRDD = i2.map(value -> {
            Set<NGramm> ngrams = new HashSet<NGramm>();
            value._2().iterator().forEachRemaining(it -> ngrams.add(it.getNGramm()));
            Match mv = value._2().iterator().next();
            return new Main.MergedVector(mv.getVectors(), mv.template, ngrams, ngrams.size());
        });

        return mergedVectorRDD;

    }

    private static RowMatrix computeRowmatrixViaTranspose(JavaPairRDD<Vector, Long> indexedVectors){
        JavaRDD<IndexedRow> rows = indexedVectors.map(tuple -> new IndexedRow(tuple._2(), tuple._1()));

        IndexedRowMatrix mat = new IndexedRowMatrix(rows.rdd());
        return transpose.transposeRowMatrix(mat);
    }

    private static RowMatrix computeRowmatrixViaCoordMat(JavaPairRDD<Vector, Long> indexedVectors){

        JavaRDD<MatrixEntry> asdf = indexedVectors.flatMap(vectorLongTuple2 -> {
            double[] vector = vectorLongTuple2._1().toArray();
            List<MatrixEntry> list = new LinkedList<>();
            for (long i=0; i< vector.length; i++){
                list.add(new MatrixEntry(i, vectorLongTuple2._2().longValue(), vector[(int) i]));
            }
            return list;
        });

        CoordinateMatrix cmat = new CoordinateMatrix(asdf.rdd());

        return cmat.toRowMatrix();
    }
}
