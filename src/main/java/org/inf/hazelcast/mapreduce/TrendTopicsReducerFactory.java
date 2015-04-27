package org.inf.hazelcast.mapreduce;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class TrendTopicsReducerFactory implements ReducerFactory<String, Integer, Integer> {

	private static final long serialVersionUID = 1L;

	public Reducer<Integer, Integer> newReducer(String key) {
        return new WordcountReducer();
    }

    private static class WordcountReducer
            extends Reducer<Integer, Integer> {

        private volatile int count;

        @Override
        public void reduce(Integer value) {
            count += value;
        }

        @Override
        public Integer finalizeReduce() {
            return count == 0 ? null : count;
        }
    }
}