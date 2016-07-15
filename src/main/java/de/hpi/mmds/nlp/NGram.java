package de.hpi.mmds.nlp;

import de.hpi.mmds.nlp.template.Template;
import edu.stanford.nlp.ling.TaggedWord;

import java.io.Serializable;
import java.util.List;

public class NGram implements Serializable {
    public List<TaggedWord> taggedWords;
    public Template template;

    public NGram(List<TaggedWord> twords, Template template) {
        this.taggedWords = twords;
        this.template = template;
    }
}
