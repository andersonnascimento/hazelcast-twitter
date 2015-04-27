package org.inf.hazelcast.main;

import org.inf.hazelcast.trendtopics.HazelcastProcessing;
import org.inf.hazelcast.trendtopics.TwitterStreamRetriever;

public class Start {
	
	public static void main(String args[]){
		String url = "";
		String type = "";
		String ip = "";
		String networkInterface = "";
		
		if (args.length > 0) {
			type = args[0];
		}
		if (args.length > 1) {
			url = args[1];
		}
		if (args.length > 2) {
			ip = args[2];
		}
		if (args.length > 3) {
			networkInterface = args[3];
		}

		if (ip.equals("")) {
			ip = "127.0.0.1";
		}
	
		if (type.toLowerCase().startsWith("p")){
			HazelcastProcessing processing = new HazelcastProcessing();
			processing.start(url, ip, networkInterface);
		}
		else if (type.toLowerCase().startsWith("r")){
			TwitterStreamRetriever stream = new TwitterStreamRetriever();
			stream.start(url, ip, networkInterface);
		}
		else {
			System.out.println("Invalid parameters");
		}
	}
}
