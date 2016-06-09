package de.hpi.mmds.nlp.template;

import de.hpi.mmds.nlp.BigramThesis;
import edu.stanford.nlp.ling.TaggedWord;

public class AdjectiveVerbTemplate extends Template {

    private int state = 0;

    @Override
    public boolean matches(TaggedWord word) {
        if (state == 0 && BigramThesis.adjectiveTags.contains(word.tag())) {
            state = 1;
            return false;
        } else if (state == 1 && BigramThesis.verbTags.contains(word.tag())) {
            state = 0;
            return true;
        } else {
            state = 0;
            return false;
        }
    }
}
