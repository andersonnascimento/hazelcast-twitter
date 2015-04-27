package org.inf.hazelcast.mapreduce;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class TrendTopicsCombinerFactory implements CombinerFactory<String, Integer, Integer> {

	private static final long serialVersionUID = 1L;

	public Combiner<Integer, Integer> newCombiner(String key) {
        return new WordcountCombiner();
    }

    private static class WordcountCombiner extends Combiner<Integer, Integer> {
        private int count;

        @Override
        public void combine(Integer value) {
            count += value;
        }

        @Override
        public Integer finalizeChunk() {
            return count == 0 ? null : count;
        }

        @Override
        public void reset() {
            count = 0;
        }
    }
}
