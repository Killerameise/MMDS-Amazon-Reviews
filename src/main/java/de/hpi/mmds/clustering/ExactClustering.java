package de.hpi.mmds.clustering;

import de.hpi.mmds.nlp.Match;
import de.hpi.mmds.nlp.MergedVector;
import de.hpi.mmds.nlp.NGram;
import de.hpi.mmds.nlp.VectorWithWords;
import de.hpi.mmds.nlp.template.Template;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExactClustering implements NGramClustering {

    @Override
    public JavaRDD<MergedVector> resolveDuplicates(JavaPairRDD<Match, Integer> repartitionedVectorRDD,
                                                   Double threshold, JavaSparkContext context, Integer CPUS) {
        return repartitionedVectorRDD.aggregateByKey(0, (a, b) -> a + b, (a, b) -> a + b).map((t) -> {
            List<VectorWithWords> vectors = t._1().getVectors();
            Template template = t._1().getTemplate();
            Set<NGram> ngrams = new HashSet<>();
            ngrams.add(t._1().getNGramm());
            Integer count = t._2();
            return new MergedVector(vectors, template, ngrams, count);
        });
    }

}
