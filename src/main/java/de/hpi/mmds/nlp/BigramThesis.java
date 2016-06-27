package de.hpi.mmds.nlp;

import de.hpi.mmds.nlp.template.Template;
import edu.stanford.nlp.ling.TaggedWord;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class BigramThesis implements Serializable {

    public static final List<String> adjectiveTags = Arrays.asList("J", "JJR", "JJS");
    public static final List<String> nounTags = Arrays.asList("NN", "NNS", "NNP", "NNPS");
    public static final List<String> adverbTags = Arrays.asList("RB", "RBR", "RBS");
    public static final List<String> verbTags = Arrays.asList("VB", "VBD");


    public static List<Tuple2<List<TaggedWord>, Integer>> findKGramsEx(int k, List<TaggedWord> text, Template template) {
        CircularFifoQueue<TaggedWord> queue = new CircularFifoQueue<>(k);
        List<Tuple2<List<TaggedWord>, Integer>> result = new LinkedList<>();

        Template.MatchResult matchResult = new Template.MatchResult(0, false, null, null);
        for (TaggedWord currentWord : text) {
            queue.add(currentWord);
            if (queue.isAtFullCapacity()) {
                matchResult = template.matches(currentWord, matchResult);
                if (matchResult.accepts) {
                    result.add(new Tuple2<>(Arrays.asList(queue.toArray(new TaggedWord[k])), 1));
                    break;
                }
            }
        }
        return result;
    }
}
