package de.hpi.mmds.filter;

import de.hpi.mmds.database.MetadataRecord;
import org.apache.spark.api.java.JavaRDD;

public class BrandFilter extends Filter {

    private String brandName;

    public BrandFilter(JavaRDD<MetadataRecord> inputRDD, String brandName) {
        super(inputRDD);
        this.brandName = brandName;
    }

    @Override
    protected boolean accepts(MetadataRecord record) {
        return record.getBrand() != null && record.getBrand().equals(this.brandName);
    }
}
