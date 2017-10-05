package com.hortonworks.historian;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.hortonworks.historian.domain.AppProps;
import com.hortonworks.historian.kafka.AtlasObserver;
import com.hortonworks.historian.model.Atlas;

@SpringBootApplication
public class HistorianWebApp {
	
	public static void main(String[] args) {
		ApplicationContext app = SpringApplication.run(HistorianWebApp.class, args);
		
		AppProps props = (AppProps) app.getBean(AppProps.class);
		System.out.println(props.getHostName());
		
		Atlas.init();
		
		
		Map<String, String> env = System.getenv();
		if(env.get("API_HOST") != null){
        	props.setHistorianApiHost((String)env.get("API_HOST"));
        }
        if(env.get("API_PORT") != null){
        	props.setHistorianApiPort((String)env.get("API_PORT"));
        }
		
		if(env.get("KAFKA_HOST") != null){
        	//kafkaHost = (String)env.get("KAFKA_HOST");
			props.setKafkaHost((String)env.get("KAFKA_HOST"));
        }
        if(env.get("KAFKA_PORT") != null){
        	//kafkaPort = (String)env.get("KAFKA_PORT");
        	props.setKafkaPort((String)env.get("KAFKA_PORT"));
        }
        
        if(env.get("ZK_HOST") != null){
        	//zkHost = (String)env.get("ZK_HOST");
        	props.setZkHost((String)env.get("ZK_HOST"));
        }
        if(env.get("ZK_PORT") != null){
        	//zkPort = (String)env.get("ZK_PORT");
        	props.setZkPort((String)env.get("ZK_PORT"));
        }
		
        System.out.println("Cronus API Host: "+props.getHistorianApiHost());
		System.out.println("Cronus API Port: "+props.getHistorianApiPort());
		System.out.println("Atlas Host: "+props.getAltasHost());
		System.out.println("Atlas Port: "+props.getAltasPort());
		System.out.println("Kafka Host: "+ props.getKafkaHost());
		System.out.println("Kafka Port: " + props.getKafkaPort());
		
		// Kafka listener
		System.out.println("********** Connecting to Kafka...");
		int numConsumers = 1;
		String groupId = "atlas-observer-group";
		List<String> topics = Arrays.asList("ATLAS_ENTITIES");
		ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
		final List<AtlasObserver> consumers = new ArrayList<>();
		for (int i = 0; i < numConsumers; i++) {
			System.out.println("********** Creating Consumer Task...");
			AtlasObserver consumer = new AtlasObserver(i, groupId, topics, props.getKafkaHost(), props.getKafkaPort(), props.getZkHost(), props.getZkPort());
			System.out.println("********** Task Created...");
			consumers.add(consumer);
		    executor.submit(consumer);
		}
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
		    public void run() {
				for (AtlasObserver consumer : consumers) {
		        consumer.shutdown();
				} 
				executor.shutdown();
				try {
					executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}
}