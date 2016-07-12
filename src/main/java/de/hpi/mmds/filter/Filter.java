package de.hpi.mmds.filter;

import de.hpi.mmds.database.MetadataRecord;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.List;

public class Filter implements Serializable {

    private JavaRDD<MetadataRecord> metadataRDD;


    public Filter(JavaRDD<MetadataRecord> inputRDD) {
        this.metadataRDD = inputRDD;
    }

    protected boolean accepts(MetadataRecord record) {
        return true;
    }

    public JavaRDD<MetadataRecord> chain() {
        return this.metadataRDD.filter(this::accepts);
    }

    public JavaRDD<String> toStringRDD() {
        return this.chain().map(MetadataRecord::getAsin);
    }

    public void toFile(String outputFileName) {
        this.metadataRDD.saveAsTextFile(outputFileName);
    }

    public List<String> toList() {
        return this.toStringRDD().collect();
    }

}

