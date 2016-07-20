package de.hpi.mmds.nlp;

import de.hpi.mmds.nlp.template.Template;
import de.hpi.mmds.nlp.template.TemplateBased;
import edu.stanford.nlp.ling.TaggedWord;

import java.io.Serializable;
import java.util.*;

public class MergedVector implements Serializable, TemplateBased {
    public List<VectorWithWords> vector;
    public Template template;
    public String feature;
    public Map<String, Integer> descriptions;
    public List<NGram> ngrams;
    public NGram representative;
    public Integer count;

    public MergedVector(final List<VectorWithWords> vector,
                        final Template template,
                        final List<NGram> ngrams,
                        final Integer count) {
        this.vector = vector;
        this.template = template;
        this.ngrams = ngrams;
        this.count = count;
        List<TaggedWord> words = new LinkedList<>();
        this.vector.forEach(v -> words.add(v.word));
        this.feature = template.getFeature(words);
        this.descriptions = new HashMap<>();
        for (NGram ngram : this.ngrams) {
            int modifierCount = 1;
            if (this.descriptions.containsKey(ngram.template.getDescription(ngram.taggedWords)))
                modifierCount += this.descriptions.get(ngram.template.getDescription(ngram.taggedWords));
            this.descriptions.put(ngram.template.getDescription(ngram.taggedWords), modifierCount);
        }
        this.representative = this.ngrams.iterator().next();
    }

    @Override
    public Template getTemplate() {
        return template;
    }

    @Override
    public NGram getNGramm() {
        return representative;
    }

    @Override
    public List<VectorWithWords> getVectors() {
        return vector;
    }
}
