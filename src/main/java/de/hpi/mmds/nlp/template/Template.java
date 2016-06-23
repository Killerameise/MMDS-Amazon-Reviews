package de.hpi.mmds.nlp.template;

import edu.stanford.nlp.ling.TaggedWord;
import org.apache.commons.collections4.queue.CircularFifoQueue;

import java.io.Serializable;

public abstract class Template implements Serializable {

    public boolean matches(CircularFifoQueue<TaggedWord> queue) {
        MatchResult matchResult = new MatchResult(0, false);
        for (TaggedWord tw : queue) {
            matchResult = matches(tw, matchResult);
            if (matchResult.accepts) return true;
        }
        return false;
    }

    public abstract MatchResult matches(TaggedWord word, MatchResult previous);

    public abstract String getFeature(CircularFifoQueue<TaggedWord> queue);


    public static class MatchResult {

        public Integer state;
        public Boolean accepts;

        public MatchResult(Integer state, Boolean accepts) {
            this.state = state;
            this.accepts = accepts;
        }
    }

}
