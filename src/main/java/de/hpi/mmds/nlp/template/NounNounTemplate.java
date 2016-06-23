package de.hpi.mmds.nlp.template;

import de.hpi.mmds.nlp.BigramThesis;
import edu.stanford.nlp.ling.TaggedWord;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class NounNounTemplate extends Template {

    @Override
    public MatchResult matches(TaggedWord word, MatchResult previous) {
        if (previous.state == 0 && BigramThesis.adjectiveTags.contains(word.tag())) {
            return new MatchResult(1, false);
        } else if (previous.state == 1 && BigramThesis.nounTags.contains(word.tag())) {
            // Keep returning true until something else than a noun comes along
            return new MatchResult(1, true);
        } else {
            return new MatchResult(0, false);
        }
    }

    @Override
    public String getFeature(Collection<TaggedWord> queue) {
        List<String> result = new LinkedList<>();
        MatchResult matchResult = new MatchResult(0, false);
        for (TaggedWord taggedWord : queue) {
            matchResult = matches(taggedWord, matchResult);
            if (matchResult.accepts) {
                result.add(taggedWord.word());
            }
        }
        return String.join(" ", result);
    }
}
