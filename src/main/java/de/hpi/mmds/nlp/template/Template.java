package de.hpi.mmds.nlp.template;

import edu.stanford.nlp.ling.TaggedWord;
import org.apache.commons.collections4.queue.CircularFifoQueue;

import javax.swing.text.html.HTML;
import java.io.Serializable;
import java.util.Collection;

public abstract class Template implements Serializable {

    public boolean matches(CircularFifoQueue<TaggedWord> queue) {
        MatchResult matchResult = new MatchResult(0, false, null, null);
        for (TaggedWord tw : queue) {
            matchResult = matches(tw, matchResult);
            if (matchResult.accepts) return true;
        }
        return false;
    }

    public abstract MatchResult matches(TaggedWord word, MatchResult previous);

    public abstract String getFeature(Collection<TaggedWord> queue);

    public abstract String getDescription(Collection<TaggedWord> queue);


    public static class MatchResult {

        public Integer state;
        public Boolean accepts;
        public TaggedWord feature;
        public TaggedWord description;

        public MatchResult(Integer state, Boolean accepts, TaggedWord feature, TaggedWord description) {
            this.state = state;
            this.accepts = accepts;
            this.feature=feature;
            this.description=description;
        }
    }

}
