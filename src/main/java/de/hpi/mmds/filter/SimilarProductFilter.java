package de.hpi.mmds.filter;

import de.hpi.mmds.database.MetadataRecord;
import org.apache.spark.api.java.JavaRDD;

import java.util.Collection;

public class SimilarProductFilter extends Filter {

    private String asin;

    public SimilarProductFilter(JavaRDD<MetadataRecord> inputRDD, String asin) {
        super(inputRDD);
        this.asin = asin;
    }

    @Override
    protected boolean accepts(MetadataRecord record) {
        MetadataRecord.Related products = record.getRelated();
        if (products == null) return false;
        Collection<String> alsoBought = products.also_bought;
        Collection<String> boughtTogether = products.bought_together;
        boolean result = asin.equals(record.getAsin());
        result |= (alsoBought != null && alsoBought.contains(asin));
        result |= (boughtTogether != null && boughtTogether.contains(asin));
        return result;
    }
}
