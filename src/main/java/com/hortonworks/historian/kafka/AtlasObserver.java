package com.hortonworks.historian.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.historian.model.Atlas;

public class AtlasObserver implements Runnable {
	  
	private final ObjectMapper mapper = new ObjectMapper();
	private final KafkaConsumer<String, String> consumer;
	private final List<String> topics;
	private final int id;
	  
	public AtlasObserver(int id,String groupId, List<String> topics, String kafkaHost, String kafkaPort, String zkHost, String zkPort) {
		System.out.println("********** Initializing AtlasObserver...");
		this.id = id;
	    this.topics = topics;
	    Properties props = new Properties();
	    props.put("bootstrap.servers", kafkaHost+":"+kafkaPort);
	    props.put("group.id", groupId);
	    props.put("key.deserializer", StringDeserializer.class.getName());
	    props.put("value.deserializer", StringDeserializer.class.getName());
	    this.consumer = new KafkaConsumer<>(props);
	}
	  
	@Override
	public void run() {
		boolean skipFlag = false;
		try {
			consumer.subscribe(topics);
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
	        
				for (ConsumerRecord<String, String> record : records) {
					Map<String, Object> data = new HashMap<>();
					data.put("partition", record.partition());
					data.put("offset", record.offset());
					data.put("value", record.value());
					System.out.println(this.id + ": " + data);
					
					JSONObject message = new JSONObject(record.value());
					String typeName = message.getJSONObject("message").getJSONObject("entity").getString("typeName");
					String entityName = message.getJSONObject("message").getJSONObject("entity").getJSONObject("values").getString("name");
					String operationType = message.getJSONObject("message").getString("operationType");
					JSONArray jsonTraitArray = message.getJSONObject("message").getJSONObject("entity").getJSONArray("traitNames");
					List<String> traitArray = new ArrayList<String>();
					for (int i=0; i < jsonTraitArray.length(); i++) {
					    traitArray.add(jsonTraitArray.getString(i));
					}
					System.out.println(typeName);
					System.out.println(entityName);
					System.out.println(operationType);

					System.out.println("********** Move Entity to Specified Classification...");	
					System.out.println("********** Entity Reported Classificaition: " + traitArray);
					System.out.println("********** Current Tag Classificaiton Mapping: " + Atlas.tagsTaxMapping);
					String targetLeaf = null;
					if(typeName.equalsIgnoreCase(typeName) && operationType.equalsIgnoreCase("TRAIT_ADD")){
						if(Atlas.tagsTaxMapping.containsKey(entityName) && traitArray.size() > 0){	
							traitArray.removeAll((Collection<?>) Atlas.tagsTaxMapping.get(entityName));
							System.out.println("********** Detecting Added Classificaition: " + traitArray);
							if(traitArray.size() > 0){
								targetLeaf = traitArray.get(0);
							}else{
								System.out.println("********** The requested Classification has already been assigned to this tag, skipping Apply...");
								skipFlag = true;
							}
						}if(!Atlas.tagsTaxMapping.containsKey(entityName)){
							System.out.println("********** This tag does not have any relevant Classificaitons applied, Applying...");
						}else{
							System.out.println("********** The requested Classification has already been assigned to this tag, skipping Apply...");
							skipFlag = true;
						}
					}else if(typeName.equalsIgnoreCase(typeName) && operationType.equalsIgnoreCase("TRAIT_DELETE")){
						if(traitArray.size() > 0){
							((Collection<?>) Atlas.tagsTaxMapping.get(entityName)).removeAll(traitArray);
							System.out.println("********** Detecting Deleted Classification: " + Atlas.tagsTaxMapping.get(entityName));
							targetLeaf = ((List)Atlas.tagsTaxMapping.get(entityName)).get(0).toString();
						}else{
							System.out.println("********** There are no Classifications assigned to this tag, skipping Delete...");
							skipFlag = true;
						}
					}
					
					if(skipFlag == false){	
						List<Map<String,Object>> currentNode = (List<Map<String, Object>>) ((HashMap)Atlas.atlasFileTree.get("core")).get("data");
						System.out.println("********** Updated File Tree: "+updateFileTree(entityName, targetLeaf+"."+entityName, targetLeaf, currentNode, operationType));
						Atlas.tagsTaxMapping.put(entityName, traitArray);
					}
					skipFlag = false;
				}	
			}
		} catch (JSONException e) {
			System.out.println("There was a problem parsing the incoming JSON event. "
							 + "This is could be because a UI interaction was replayed as an event. "
							 + "If so, it's safe to ignore this message. \n"
							 + e.getMessage());
		} catch (WakeupException e){
			System.out.println(" Kafka Consumer Wakeup Exception... " + e.getMessage());
		} catch (Exception e){
			e.printStackTrace();	
		}finally {
			//consumer.close();
	    }
	}
	
	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> updateFileTree(String entityName, String fullPath, String targetLeaf, List<Map<String,Object>> currentNode, String operationType){
		boolean isTargetLeaf = false;
		String currentBranch;
		if(targetLeaf.contains(".")){
			currentBranch = targetLeaf.split("\\.")[0];
		}else{
			isTargetLeaf = true;
			currentBranch = targetLeaf;
		}
		
		Iterator<Map<String, Object>> nodeIterator = currentNode.iterator();
		List<Map<String,Object>> updatedNode = new ArrayList<Map<String,Object>>(); 
		while(nodeIterator.hasNext()){
			HashMap<String,Object>currentLeaf = (HashMap<String, Object>) nodeIterator.next();
			if(currentLeaf.get("text").toString().equalsIgnoreCase(currentBranch)){
				if(isTargetLeaf){
					Map<String,Object> entity = new HashMap<String,Object>();
					entity.put("id", fullPath);
					entity.put("text", entityName);
					entity.put("type", "tag");
					if(currentLeaf.containsKey("children")){
						updatedNode = ((List<Map<String,Object>>)currentLeaf.get("children"));
					}else{
						currentLeaf.put("children", new ArrayList<Map<String,Object>>());
						updatedNode = ((List<Map<String,Object>>)currentLeaf.get("children"));
					}
					if(operationType.equalsIgnoreCase("TRAIT_ADD")){
						updatedNode.add(entity);
					}else if(operationType.equalsIgnoreCase("TRAIT_DELETE")){
						updatedNode.remove(updatedNode.indexOf(entity));	
					}
					return updatedNode;
				}else{
					targetLeaf = targetLeaf.split(currentBranch+".")[1];
					List<Map<String,Object>> nextLeaf = (List<Map<String,Object>>)currentLeaf.get("children");
					updatedNode = updateFileTree(entityName, fullPath, targetLeaf, nextLeaf, operationType);	
				}
			}
		}
		return updatedNode;
	}
	
	public void shutdown() {
		consumer.wakeup();
	}
}
