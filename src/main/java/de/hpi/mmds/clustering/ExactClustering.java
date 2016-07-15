package de.hpi.mmds.clustering;

import de.hpi.mmds.Main;
import de.hpi.mmds.nlp.template.Template;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExactClustering implements NGramClustering {

    @Override
    public JavaRDD<Main.MergedVector> resolveDuplicates(JavaPairRDD<Main.Match, Integer> repartitionedVectorRDD,
                                                        Double threshold, JavaSparkContext context, Integer CPUS) {
        return repartitionedVectorRDD.aggregateByKey(0, (a, b) -> a + b, (a, b) -> a + b).map((t) -> {
            List<Main.VectorWithWords> vectors = t._1().getVectors();
            Template template = t._1().getTemplate();
            Set<Main.NGramm> ngrams = new HashSet<>();
            ngrams.add(t._1().getNGramm());
            Integer count = t._2();
            return new Main.MergedVector(vectors, template, ngrams, count);
        });
    }

}
