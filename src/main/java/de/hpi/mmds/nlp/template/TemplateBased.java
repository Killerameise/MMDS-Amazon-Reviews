package de.hpi.mmds.nlp.template;

import de.hpi.mmds.nlp.NGram;
import de.hpi.mmds.nlp.VectorWithWords;

import java.util.List;

public interface TemplateBased {
    Template getTemplate();

    NGram getNGramm();

    List<VectorWithWords> getVectors();
}
