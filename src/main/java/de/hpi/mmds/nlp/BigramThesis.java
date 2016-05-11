package de.hpi.mmds.nlp;

import java.util.*;
import java.util.stream.Stream;

import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.stats.ClassicCounter;

import javax.swing.text.html.HTML;

/**
 * Created by axel on 10.05.16.
 * We look for adjectives/ adverbs (POS-tag JJ, JJR, JJS, RB, RBR, RBS) followed by a substantive (NN, NNS, NNP, NNPS)
 *
 */
public class BigramThesis {

    public static List<String> adjectiveTags = Arrays.asList("JJR");//("J", "JJR", "JJS");
    public static List<String> nounTags = Arrays.asList("NN", "NNS", "NNP", "NNPS");
    public static List<String> adverbTags = Arrays.asList("RBR");//"RB", "RBR", "RBS");
    public ClassicCounter<List<TaggedWord>> bigramCounter;



    public BigramThesis(){
        bigramCounter = new ClassicCounter<>();
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
