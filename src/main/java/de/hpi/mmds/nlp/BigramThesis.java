package de.hpi.mmds.nlp;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;

import edu.stanford.nlp.ling.Tag;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.stats.ClassicCounter;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import scala.Tuple2;

import javax.swing.text.html.HTML;

/**
 * Created by axel on 10.05.16.
 * We look for adjectives/ adverbs (POS-tag JJ, JJR, JJS, RB, RBR, RBS) followed by a substantive (NN, NNS, NNP, NNPS)
 *
 */
public class BigramThesis implements Serializable {

    public static List<String> adjectiveTags = Arrays.asList("J", "JJR", "JJS");
    public static List<String> nounTags = Arrays.asList("NN", "NNS", "NNP", "NNPS");
    public static List<String> adverbTags = Arrays.asList("RB", "RBR", "RBS");
    public ClassicCounter<List<TaggedWord>> bigramCounter;
    public Map<Integer, ClassicCounter<List<TaggedWord>>> xGramCounter;



    public BigramThesis(){
        bigramCounter = new ClassicCounter<>();
        xGramCounter = new HashMap<>();
    }

    public void findBigrams(List<TaggedWord> posList){
        Iterator<TaggedWord> iterator = posList.iterator();
        TaggedWord lastToken = null;
        while (iterator.hasNext()){
            TaggedWord currentToken = iterator.next();
            if (lastToken != null) {
                if ((adjectiveTags.contains(lastToken.tag()) || adverbTags.contains(lastToken.tag()))&& nounTags.contains(currentToken.tag())){
                    List<TaggedWord> bigram = Arrays.asList(lastToken, currentToken);
                    bigramCounter.incrementCount(bigram);
                }

            }
            lastToken = currentToken;
        }
    }

    public void findXGrams(int x, List<TaggedWord> text){
        for (int i=2; i<=x; i++){
            if (xGramCounter.containsKey(i)){
                xGramCounter.get(i).addAll(findKGrams(i, text));
            }
            else {
                xGramCounter.put(i, findKGrams(i, text));
            }
        }
    }

    public static List<Tuple2<List<TaggedWord>, Double>> convertCounter(ClassicCounter<List<TaggedWord>> counter) {
        List<Tuple2<List<TaggedWord>, Double>> result = new LinkedList<>();
        for (List<TaggedWord> l: counter){
            result.add(new Tuple2<>(l, counter.getCount(l)));
        }
        return result;
    }

    public static ClassicCounter<List<TaggedWord>> combineCounters(ClassicCounter<List<TaggedWord>> a, ClassicCounter<List<TaggedWord>> b) {
        ClassicCounter<List<TaggedWord>> result = new ClassicCounter<>();
        result.addAll(a);
        result.addAll(b);
        return result;
    }

    public static List<Tuple2<List<TaggedWord>, Integer>> findKGramsEx(int k, List<TaggedWord> text){
        CircularFifoQueue<TaggedWord> queue = new CircularFifoQueue<>(k);
        List<Tuple2<List<TaggedWord>, Integer>> result = new LinkedList<>();
        for (TaggedWord current_word : text) {
            queue.add(current_word);
            if (queue.isAtFullCapacity()) {
                Boolean containsAdj = false;
                Boolean containsNoun = false;
                for (TaggedWord token : queue) {
                    if (adjectiveTags.contains(token.tag()) || adverbTags.contains(token.tag())) {
                        containsAdj = true;
                    } else if (nounTags.contains(token.tag())) {
                        containsNoun = true;
                    }
                    if (containsAdj && containsNoun) {
                        result.add(new Tuple2<>(Arrays.asList(queue.toArray(new TaggedWord[k])), 1));
                        break;
                    }
                }
            }
        }
        return result;
    }

    public ClassicCounter<List<TaggedWord>> findKGrams(int k, List<TaggedWord> text){
        CircularFifoQueue<TaggedWord> queue = new CircularFifoQueue<>(k);
        ClassicCounter<List<TaggedWord>> counter = new ClassicCounter<>();
        for (int i=0; i<text.size(); i++){
            TaggedWord current_word= text.get(i);
            queue.add(current_word);
            if (queue.isAtFullCapacity()){
                Boolean containsAdj = false;
                Boolean containsNoun = false;
                Iterator<TaggedWord> it = queue.iterator();
                while (it.hasNext()){
                    TaggedWord token = it.next();
                    if (adjectiveTags.contains(token.tag())||adverbTags.contains(token.tag())){
                        containsAdj = true;
                    }
                    else if (nounTags.contains(token.tag())){
                        containsNoun = true;
                    }
                }
                if (containsAdj && containsNoun){
                    counter.incrementCount(Arrays.asList(queue.toArray(new TaggedWord[k])));
                }
            }
        }
        return counter;
    }

    public Map<List<TaggedWord>, Double> getKCommonXGrams(int k, int x){
        Map<List<TaggedWord>, Double> unsortedMap = new HashMap<>();
        for (List<TaggedWord> l: xGramCounter.get(x)){
            unsortedMap.put(l, xGramCounter.get(x).getCount(l));
        }
        Map<List<TaggedWord>, Double> sortedMap = sortByValue(unsortedMap);
        Iterator<Map.Entry<List<TaggedWord>, Double>> iterator = sortedMap.entrySet().iterator();
        Integer i = k;
        LinkedHashMap<List<TaggedWord>, Double> result = new LinkedHashMap();
        while (iterator.hasNext() && i>0) {
            Map.Entry<List<TaggedWord>, Double> item = iterator.next();
            result.put(item.getKey(), item.getValue());
            i -= 1;
        }
        return result;
    }


    public Map<List<TaggedWord>, Double> getKCommonBigrams(int k){
        Map<List<TaggedWord>, Double> unsortedMap = new HashMap<>();
        for (List<TaggedWord> l: bigramCounter){
            unsortedMap.put(l, bigramCounter.getCount(l));
        }
        Map<List<TaggedWord>, Double> sortedMap = sortByValue(unsortedMap);
        Iterator<Map.Entry<List<TaggedWord>, Double>> iterator = sortedMap.entrySet().iterator();
        Integer i = k;
        LinkedHashMap<List<TaggedWord>, Double> result = new LinkedHashMap();
        while (iterator.hasNext() && i>0) {
            Map.Entry<List<TaggedWord>, Double> item = iterator.next();
            result.put(item.getKey(), item.getValue());
            i -= 1;
        }
        return result;
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue( Map<K, V> map )
    {
        Map<K, V> result = new LinkedHashMap<>();
        Stream<Map.Entry<K, V>> st = map.entrySet().stream();

        st.sorted( Map.Entry.<K, V>comparingByValue().reversed() )
                .forEachOrdered(e -> result.put(e.getKey(), e.getValue()));

        return result;
    }





}
