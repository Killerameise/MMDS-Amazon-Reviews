package de.hpi.mmds.filter;

import de.hpi.mmds.database.MetadataRecord;
import org.apache.spark.api.java.JavaRDD;

public class SampleFilter extends Filter {

    public SampleFilter(JavaRDD<MetadataRecord> inputRDD, double fraction, long seed) {
        super(inputRDD.sample(false, fraction, seed));
    }

}
