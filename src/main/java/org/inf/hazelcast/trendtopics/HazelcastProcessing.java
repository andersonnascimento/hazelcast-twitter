package org.inf.hazelcast.trendtopics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.inf.hazelcast.mapreduce.TokenizerMapper;
import org.inf.hazelcast.mapreduce.TrendTopicsReducerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

public class HazelcastProcessing {
	private static HazelcastInstance hazelcastInstance = null;
	private String urlManagement = "";
	private String ip = "";
	private String networkInterface = "";
		
	private HazelcastInstance startHazelcastCluster(){
		Config config = new Config();
		if (urlManagement.equals("") ) {
			ManagementCenterConfig mcc = new ManagementCenterConfig();
			config.setManagementCenterConfig(mcc.setUrl(urlManagement));
		}
		Set<String> n = new HashSet<String>();
		n.add(networkInterface);
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        if (!networkConfig.equals(""))
        	networkConfig.getJoin().getMulticastConfig().setTrustedInterfaces(n);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);
        networkConfig.getJoin().getTcpIpConfig().setMembers(Arrays.asList(new String[]{ip}));

        return Hazelcast.newHazelcastInstance(config);
	}
	
	public void start(String urlManagement, String ip, String networkInterface) {
		this.ip = ip;
		this.networkInterface = networkInterface;
		
		hazelcastInstance = startHazelcastCluster();
		IQueue<String> hzQueue = hazelcastInstance.getQueue(THZConstants.HAZELCAST_QUEUE_NAME);
		JobTracker tracker = hazelcastInstance.getJobTracker(THZConstants.HAZELCAST_JOB_TRACKER);

        long interval = System.currentTimeMillis();

		int count_block = 0;
		while (true) {
			if (System.currentTimeMillis() - interval > 15000) {
				long timeStep = System.currentTimeMillis();
				IMap<String, String> map = getIMapList(hzQueue);
		        
		        if (map != null && map.size() > 0) {
		        	KeyValueSource<String, String> source = KeyValueSource.fromMap(map);
			        Job<String, String> job = tracker.newJob(source);
			        
			        ICompletableFuture<Map<String, Integer>> future = job.mapper(new TokenizerMapper())
			        											.reducer(new TrendTopicsReducerFactory())
			        											.submit();
					
			        long mergingTime = System.currentTimeMillis();
					try {
						mergingResults(future.get());
					} catch (Exception e) {
						System.out.println(e.getStackTrace());
					}
					System.out.println("Map reduce data (count, merge, whole): "+count_block+","+ (System.currentTimeMillis() - mergingTime) + "," + (System.currentTimeMillis() - timeStep));
		        }
		        else {
		        	System.out.println("No data to process.");
		        }
		        interval = System.currentTimeMillis();
			}
		}
	}
	
	private void mergingResults(Map<String, Integer> results){
		IMap<String, Integer> map = hazelcastInstance.getMap(THZConstants.HAZELCAST_FINAL_MAP);
		for (Entry<String, Integer> element : results.entrySet()) {
			if (map.containsKey(element.getKey())) {
				map.put(element.getKey(), map.get(element.getKey()) + element.getValue());
			}
			else {
				map.put(element.getKey(), element.getValue());
			}
		}		
	}
	
	private IMap<String, String> getIMapList(IQueue<String> hzQueue){
		 IMap<String, String> map = null; 
		 java.util.List<String> listTweets = new ArrayList<String>() ; 
		 hzQueue.drainTo(listTweets, 50);
	     if (listTweets.size() > 0) {
	    	map = hazelcastInstance.getMap(THZConstants.HAZELCAST_QUEUE_MAP);
	    	 
	    	int count = 0;
			for (String tweet : listTweets) {
				map.put(""+count, tweet);
				count++;
			}
	     }
	     System.out.println("Drained " + listTweets.size() + " of " + hzQueue.size());
	     return map;
	}
}