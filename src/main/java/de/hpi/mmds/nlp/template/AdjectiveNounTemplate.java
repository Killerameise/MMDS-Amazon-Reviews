package de.hpi.mmds.nlp.template;

import de.hpi.mmds.nlp.BigramThesis;
import edu.stanford.nlp.ling.TaggedWord;

import java.util.Collection;


public class AdjectiveNounTemplate extends Template {

    @Override
    public MatchResult matches(TaggedWord word, MatchResult previous) {
        if (previous.state == 0 && word.tag().equals("JJR")) {
            return new MatchResult(1, false);
        } else if (previous.state == 1 && BigramThesis.nounTags.contains(word.tag())) {
            return new MatchResult(1, true);
        } else {
            return new MatchResult(0, false);
        }
    }

    @Override
    public String getFeature(Collection<TaggedWord> queue) {
        MatchResult matchResult = new MatchResult(0, false);
        for (TaggedWord taggedWord : queue) {
            matchResult = matches(taggedWord, matchResult);
            if (matchResult.accepts) {
                return taggedWord.word();
            }
        }
        return null;
    }

}
