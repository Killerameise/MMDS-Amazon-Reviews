package de.hpi.mmds.clustering;

import de.hpi.mmds.nlp.Match;
import de.hpi.mmds.nlp.MergedVector;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public interface NGramClustering {

    JavaRDD<MergedVector> resolveDuplicates(JavaPairRDD<Match, Integer> repartitionedVectorRDD,
                                            Double threshold, JavaSparkContext context, Integer CPUS);

}
