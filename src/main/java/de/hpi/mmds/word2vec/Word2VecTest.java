package de.hpi.mmds.word2vec;

import de.hpi.mmds.database.ReviewRecord;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.CollectionSentenceIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.sentenceiterator.SentencePreProcessor;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.EndingPreProcessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Jaspar Mang on 27.05.16.
 */
public class Word2VecTest {
    public static void testWord2Vec(List<ReviewRecord> reviewRecords) {
        Collection<String> reviewTexts = reviewRecords.stream().limit(200).map(ReviewRecord::getReviewText).collect(
                Collectors.toList());
        reviewTexts.stream().limit(10).forEach(System.out::println);
        System.out.println("Size Reviews: " + reviewTexts.size() +
                           "\n Loading Data");
        //Loading Data
        SentenceIterator iter = new CollectionSentenceIterator(reviewTexts);
        iter.setPreProcessor((SentencePreProcessor) String::toLowerCase);

        //Tokenizing Data
        System.out.println("Tokenizing Data");
        final EndingPreProcessor preProcessor = new EndingPreProcessor();
        TokenizerFactory tokenizer = new DefaultTokenizerFactory();
        tokenizer.setTokenPreProcessor(token -> {
            token = token.toLowerCase();
            String base = preProcessor.preProcess(token);
            base = base.replaceAll("\\d", "d");
            return base;
        });

        //Training The Model
        System.out.println("Training the model");
        int batchSize = 1000;
        int iterations = 3;
        int layerSize = 150;

        Word2Vec vec = new Word2Vec.Builder()
                .batchSize(batchSize) //# words per minibatch.
                .minWordFrequency(5) //
                .useAdaGrad(false) //
                .layerSize(layerSize) // word feature vector size
                .iterations(iterations) // # iterations to train
                .learningRate(0.025) //
                .minLearningRate(1e-3) // learning rate decays wrt # words. floor learning
                .negativeSample(10) // sample size 10 words
                .iterate(iter) //
                .tokenizerFactory(tokenizer)
                .build();
        vec.fit();

        //Evaluate
        double sim = vec.similarity("child", "baby");
        System.out.println("Similarity between child and baby: " + sim);
        Collection<String> similar = vec.wordsNearest("day", 10);
        System.out.println("Similar words to 'baby' : " + similar);
        try(BufferedReader br = new BufferedReader(new InputStreamReader(System.in))){
            while(true){

                System.out.print("\n1 = Compare two words, 2 = Get similar words, 3 = exit.\n");
                int method = Integer.parseInt(br.readLine());
                if(method == 1){
                    System.out.print("\nFirst Word\n");
                    String word1 = br.readLine();
                    System.out.print("\nSecond Word\n");
                    String word2 = br.readLine();
                    sim = vec.similarity(word1, word2);
                    System.out.println("Similarity between " + word1 + " and " + word2 + ": " + sim);
                }else if(method == 2){
                    System.out.print("\nWord\n");
                    String word = br.readLine();
                    similar = vec.wordsNearest(word, 10);
                    System.out.println("Similar words to '"+word+"' : " + similar);
                }else{
                    break;
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
