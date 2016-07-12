package de.hpi.mmds.filter;

import de.hpi.mmds.database.MetadataRecord;
import org.apache.spark.api.java.JavaRDD;

public class CategoryFilter extends Filter {

    private String category;

    public CategoryFilter(JavaRDD<MetadataRecord> inputRDD, String category) {
        super(inputRDD);
        this.category = category;
    }

    @Override
    protected boolean accepts(MetadataRecord record) {
        String[][] categories = record.getCategories();
        if (categories == null) return false;
        for (String[] branch : categories) {
            for (String subCategory : branch) {
                if (subCategory.equals(this.category)) {
                    return true;
                }
            }
        }
        return false;
    }
}
