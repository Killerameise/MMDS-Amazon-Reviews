package de.hpi.mmds.nlp.template;

import de.hpi.mmds.nlp.BigramThesis;
import edu.stanford.nlp.ling.TaggedWord;

public class NounNounTemplate extends Template {

    private int state = 0;

    @Override
    public boolean matches(TaggedWord word) {
        if (state == 0 && BigramThesis.adjectiveTags.contains(word.tag())) {
            state = 1;
            return false;
        } else if (state == 1 && BigramThesis.nounTags.contains(word.tag())) {
            // Keep returning true until something else than a noun comes along
            return true;
        } else {
            state = 0;
            return false;
        }
    }
}
