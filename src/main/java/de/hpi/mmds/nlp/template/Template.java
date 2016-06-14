package de.hpi.mmds.nlp.template;

import edu.stanford.nlp.ling.TaggedWord;
import org.apache.commons.collections4.queue.CircularFifoQueue;

public abstract class Template {

    public boolean matches(CircularFifoQueue<TaggedWord> queue) {
        for (TaggedWord tw : queue) {
            if (matches(tw)) return true;
        }
        return false;
    }

    public abstract boolean matches(TaggedWord word);

}
