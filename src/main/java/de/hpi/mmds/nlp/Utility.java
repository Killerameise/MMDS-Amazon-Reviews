package de.hpi.mmds.nlp;

import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.WordTokenFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by axel on 07.05.16.
 */
public class Utility {

    private final static String stopwordPath = "resources/stopwords.txt";
    private static List<Word> stopwords;
    static {
        try {
            PTBTokenizer tokenizer = new PTBTokenizer<>(new FileReader(stopwordPath), new WordTokenFactory(), "");
            stopwords = new ArrayList<Word>(new HashSet<Word>(tokenizer.tokenize()));
        } catch (FileNotFoundException i) {
            System.out.println("Did not find stopword list");
            stopwords = new ArrayList<Word>();
        }
    }

    public static List<String> tokenize(String text){
        return tokenize(text, false);
    }

    public static List<String> tokenize(String text, Boolean useStopwords){
        PTBTokenizer tokenizer = new PTBTokenizer<>(new StringReader(text), new WordTokenFactory(), "");
        List<Word> wordList = tokenizer.tokenize();
        if (useStopwords){
            wordList.removeAll(stopwords);
        }
        List<String> result = new ArrayList<>();
        for(Word w : wordList) result.add(w.toString().toLowerCase());
        return result;
    }
}
