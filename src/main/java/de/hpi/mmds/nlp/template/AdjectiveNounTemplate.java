package de.hpi.mmds.nlp.template;

import de.hpi.mmds.nlp.BigramThesis;
import edu.stanford.nlp.ling.TaggedWord;

import java.util.Collection;
import java.util.Iterator;


public class AdjectiveNounTemplate extends Template {

    @Override
    public MatchResult matches(TaggedWord word, MatchResult previous) {
        if (BigramThesis.adjectiveTags.contains(word.tag())) {
            return new MatchResult(1, false, null, word);
        } else if (previous.state == 1 && BigramThesis.nounTags.contains(word.tag())) {
            return new MatchResult(1, true, word, previous.description);
        }
        else {
            return new MatchResult(0, false, null, null);
        }
    }

    @Override
    public String getFeature(Collection<TaggedWord> queue) {
        MatchResult matchResult = new MatchResult(0, false, null, null);
        for (TaggedWord taggedWord : queue) {
            matchResult = matches(taggedWord, matchResult);
            if (matchResult.accepts) {
                return matchResult.feature.word();
            }
        }
        return null;
    }

    @Override
    public String getDescription(Collection<TaggedWord> queue){
        MatchResult matchResult = new MatchResult(0, false, null, null);
        for (TaggedWord taggedWord : queue) {
            matchResult = matches(taggedWord, matchResult);
            if (matchResult.accepts) {
                return matchResult.description.word();
            }
        }
        return null;
    }

}
