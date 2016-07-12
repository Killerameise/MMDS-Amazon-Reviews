package de.hpi.mmds.filter;

import de.hpi.mmds.database.MetadataRecord;
import org.apache.spark.api.java.JavaRDD;

import javax.annotation.Nullable;

public class PriceFilter extends Filter {

    private Double priceMin, priceMax = null;

    public PriceFilter(JavaRDD<MetadataRecord> inputRDD, @Nullable Double priceMin, @Nullable Double priceMax) {
        super(inputRDD);
        this.priceMin = priceMin;
        this.priceMax = priceMax;
    }

    @Override
    protected boolean accepts(MetadataRecord record) {
        return (this.priceMin == null || this.priceMin <= record.getPrice()) &&
               (this.priceMax == null || this.priceMax >= record.getPrice());
    }
}
