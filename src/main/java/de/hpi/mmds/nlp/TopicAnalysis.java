package de.hpi.mmds.nlp;

import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.stats.ClassicCounter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;

/**
 * Created by axel on 07.06.16.
 */
public class TopicAnalysis {
    public ClassicCounter<String> wordCounter;
    public HashMap<String, Integer> wordToFeaturePos;

    public TopicAnalysis(){
        wordCounter = new ClassicCounter<>();
        wordToFeaturePos = new HashMap<>();
    }

    public void findWords(List<TaggedWord> posList){
        Iterator<TaggedWord> iterator = posList.iterator();
        while (iterator.hasNext()){
            TaggedWord word = iterator.next();
            //if (BigramThesis.nounTags.contains(word.tag()))
            wordCounter.incrementCount(word.word());
        }
    }

    public String[] getCounterAsStringArray(){
        String[] s = new String[wordCounter.size()];
        Set<String> set= wordCounter.keySet();
        Iterator<String> it = set.iterator();
        for (int i=0; i<s.length; i++){
            String word = it.next();
            wordToFeaturePos.put(word, i);
            s[i] = word;
        }
        return s;
    }

    public void removeLowFrequencyTerms(int lowestAcceptedFrequency){
        ClassicCounter<String> copyMap = new ClassicCounter<>();
        copyMap.addAll(wordCounter);
        for (Map.Entry<String, Double> entry: copyMap.entrySet()){
            if (entry.getValue()<lowestAcceptedFrequency){
                wordCounter.remove(entry.getKey());
            }
        }
    }



    /*public List<TaggedWord> getUncommonWords(){

    }*/



}
