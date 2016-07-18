package de.hpi.mmds.nlp;

import de.hpi.mmds.nlp.template.Template;
import de.hpi.mmds.nlp.template.TemplateBased;
import edu.stanford.nlp.ling.TaggedWord;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class Match implements Serializable, TemplateBased {
    public List<VectorWithWords> vectors;
    public NGram ngram;
    public Template template;
    public String representative;

    public Match(final List<VectorWithWords> vlist, final Template template) {
        this.vectors = vlist;
        this.template = template;
        List<TaggedWord> words = new LinkedList<>();
        this.vectors.forEach(a -> words.add(a.word));
        this.ngram = new NGram(words, template);
        this.representative = template.getFeature(words);
    }

    @Override
    public Template getTemplate() {
        return template;
    }

    @Override
    public NGram getNGramm() {
        return ngram;
    }

    @Override
    public List<VectorWithWords> getVectors() {
        return vectors;
    }
}
