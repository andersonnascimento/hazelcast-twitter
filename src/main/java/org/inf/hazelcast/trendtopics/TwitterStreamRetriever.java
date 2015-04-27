package org.inf.hazelcast.trendtopics;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.inf.hazelcast.model.Tweet;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hazelcast.config.Config;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterStreamRetriever {
	private String urlManagement = "";
	private String ip = "";
	private String networkInterface = "";

	private void twitterStream(HazelcastInstance hazelcastInstance, String consumerKey, String consumerSecret,String token, String secret) throws InterruptedException {
		IQueue<String> hzQueue = hazelcastInstance.getQueue(THZConstants.HAZELCAST_QUEUE_NAME);
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(50000);

		StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
		endpoint.stallWarnings(false);

		Authentication auth = new OAuth1(consumerKey, consumerSecret, token,secret);
		BasicClient client = new ClientBuilder().name("TwitterStreamProcessing").hosts(Constants.STREAM_HOST).endpoint(endpoint).authentication(auth).processor(new StringDelimitedProcessor(queue)).build();
		
		client.connect();
			
		System.out.println("Starting stream retriever");
		long time = System.currentTimeMillis();
		long interval = System.currentTimeMillis();

		StringBuilder sb = new StringBuilder();
		int countTweets = 0;
		while (true) {
			if (client.isDone()) {
				System.out.println("Client connection closed unexpectedly: "+ client.getExitEvent().getMessage());
				break;
			}

			String msg = queue.poll(5, TimeUnit.SECONDS);
			
			if (! (msg == null || msg.equals("")) ){
				JsonObject json = new JsonParser().parse(msg).getAsJsonObject();
				
				if (json.has("created_at")) {
					Tweet tweet = new Tweet();
					tweet.setCreatedAt(json.get("created_at").getAsString());
					tweet.setId(json.get("id").getAsString());
					tweet.setLang(json.get("lang").getAsString());
					tweet.setText(json.get("text").getAsString());
					
					sb.append(tweet.getText());
					countTweets++;					
				}

				if (System.currentTimeMillis() - interval > 1000){
					interval = System.currentTimeMillis();
					hzQueue.add(sb.toString());
					sb = new StringBuilder();
					
					System.out.println("Number of tweets ["+countTweets+"], [" +(System.currentTimeMillis()-time) + "]");
				}
			}
			else {
				if (System.currentTimeMillis() - interval > 1000){
					System.out.println("Twitter is not answering.");
				}
			}
		}
		client.stop();
	}

	private void startTwitterRetriever(HazelcastInstance hazelcastInstance){
		try {
			Properties twitterProperties = new Properties();
			try {
				InputStream inputStream = TwitterStreamRetriever.class.getClassLoader().getResourceAsStream(THZConstants.TWITTER_SETTINGS);
				twitterProperties.load(inputStream);
			} catch (IOException e) {
				System.out.println(e.getStackTrace());
			}
			
			twitterStream(hazelcastInstance,
			   			  twitterProperties.getProperty(THZConstants.TWITTER_CONSUMERKEY),
						  twitterProperties.getProperty(THZConstants.TWITTER_CONSUMERSECRET), 
						  twitterProperties.getProperty(THZConstants.TWITTER_TOKEN),
						  twitterProperties.getProperty(THZConstants.TWITTER_SECRET));
			
		} catch (InterruptedException e) {
			System.out.println(e.getStackTrace());
		}
	}
	
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
		this.urlManagement = urlManagement;
		this.ip = ip;
		this.networkInterface = networkInterface;
		HazelcastInstance hazelcastInstance = startHazelcastCluster();
		startTwitterRetriever(hazelcastInstance);
	}
}