package de.hpi.mmds.nlp.template;

import de.hpi.mmds.Main;

import java.util.List;

public interface TemplateBased {
    Template getTemplate();

    Main.NGramm getNGramm();

    List<Main.VectorWithWords> getVectors();
}
