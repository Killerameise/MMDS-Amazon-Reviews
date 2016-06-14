package de.hpi.mmds.word2vec;

import de.hpi.mmds.database.ReviewRecord;
import de.hpi.mmds.json.JsonReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class Word2VecTest {
    private final static String reviewPath = "resources/reviews";

    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("de.hpi.mmds").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);


        File folder = new File(reviewPath);
        File[] reviewFiles = folder.listFiles((dir, name) -> name.endsWith(".json"));

        for (File file : reviewFiles) {

            JavaRDD<String> fileRDD = jsc.textFile(file.getAbsolutePath());

            JavaRDD<ReviewRecord> recordsRDD = fileRDD.map(JsonReader::readReviewJson);

            JavaRDD<String> reviewTextRDD = recordsRDD.map(ReviewRecord::getReviewText);

            JavaRDD<Row> reviewTextRowRDD = reviewTextRDD.map(
                    (review -> RowFactory.create(Arrays.asList(review.toLowerCase().split(" ")))));

            StructType schema = new StructType(new StructField[]{
                    new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
            });


            DataFrame documentDF = sqlContext.createDataFrame(reviewTextRowRDD, schema);

            // Learn a mapping from words to Vectors.
            Word2Vec word2Vec = new Word2Vec()
                    .setInputCol("text")
                    .setOutputCol("result")
                    .setVectorSize(3)
                    .setMinCount(0);
            Word2VecModel model = word2Vec.fit(documentDF);
            System.out.println(model.getVectors().take(1));
            List<Row> synonyms = model.findSynonyms("she", 10).collectAsList();
            //System.out.println(synonyms);

        }
    }
}
