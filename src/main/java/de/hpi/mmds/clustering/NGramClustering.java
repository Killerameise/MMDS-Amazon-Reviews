package de.hpi.mmds.clustering;

import de.hpi.mmds.Main;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public interface NGramClustering {

    JavaRDD<Main.MergedVector> resolveDuplicates(JavaPairRDD<Main.Match, Integer> repartitionedVectorRDD,
                                                 Double threshold, JavaSparkContext context, Integer CPUS);

}
