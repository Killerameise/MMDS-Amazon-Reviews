package de.hpi.mmds.nlp;

import edu.stanford.nlp.ling.TaggedWord;
import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;

public class VectorWithWords implements Serializable {
    public TaggedWord word;
    public Vector vector;

    public VectorWithWords(final Vector vector, final TaggedWord words) {
        this.word = words;
        this.vector = vector;
    }

    @Override
    public String toString() {
        return "VectorWithWords{" +
                ", vector=" + vector +
                '}';
    }
}
