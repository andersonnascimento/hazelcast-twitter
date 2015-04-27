package org.inf.hazelcast.mapreduce;

import java.util.StringTokenizer;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class TokenizerMapper implements Mapper<String, String, String, Integer> {
	private static final long serialVersionUID = 1L;
	private static final Integer ONE = Integer.valueOf(1);
   
    public void map(String key, String value, Context<String, Integer> context) {
        StringTokenizer tokenizer = new StringTokenizer(value);
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken();
            if (word.startsWith("#") || word.startsWith("@"))
            	context.emit(word.toLowerCase(), ONE);
        }
    }
}
