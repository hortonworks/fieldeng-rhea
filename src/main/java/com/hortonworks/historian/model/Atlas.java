package com.hortonworks.historian.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.support.BasicAuthorizationInterceptor;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import com.hortonworks.historian.domain.AppProps;

import scala.util.parsing.json.JSON;

public abstract class Atlas {

	@Autowired
	AppProps ap;
	
	public static List<AtlasItem> atlasCache = new ArrayList<AtlasItem>();
	public static HashMap<String, Object> atlasFileTree = new HashMap<String, Object>();
	public static String atlasHost = "localhost";
	public static String atlasPort = "21000";
	public static String server = "http://"+atlasHost+":"+atlasPort;
	//public static String server = "http://historian03-499-3-2.field.hortonworks.com:21000";
	public static String api = "/api/atlas";
	//http://historian01-381-3-2:21000/api/atlas/v2/search/dsl?query=`Catalog.Mining.PA.MineA.TruckA`
	
	public static void init() {
		Map<String, String> env = System.getenv();
        System.out.println("********************** ENV: " + env);
        if(env.get("ATLAS_HOST") != null){
        	atlasHost = (String)env.get("ATLAS_HOST");
        }
        if(env.get("ATLAS_PORT") != null){
        	atlasPort = (String)env.get("ATLAS_PORT");
        }
        server = "http://"+atlasHost+":"+atlasPort;
		atlasFileTree = getFileTree();
		//setCache();
	}
   
	
	public static void setCache() {
		HashMap<String, Object> hm = getFileTree();
		
		for (String key: hm.keySet()) {
			RestTemplate rt = new RestTemplate();
	    	rt.getInterceptors().add(
	    			  new BasicAuthorizationInterceptor("admin", "admin"));  	
	    	String entityURL = server+api+"v2/search/dsl?query=`"+key+"`";  //api/atlas/v2/search/dsl?query=`Catalog.Mining.PA.MineA.TruckA`
	    	
	    	rt.setMessageConverters(Arrays.asList(new MappingJackson2HttpMessageConverter()));
	    	
	    	//org.apache.atlas.catalog.query.AtlasTaxonomyQuery
	    	
	        ResponseEntity<Object> response = rt.getForEntity(entityURL, Object.class);
	        System.out.println(response.getBody());
	    	
	    	
		}
		
		//TODO code to merge and create cache
	}
	
	
	
    public static HashMap<String, Object>  getFileTree() {
    
    	HashMap<String, Object> coreHm = new HashMap<String, Object>();
    	HashMap<String, Object> dataHm = new HashMap<String, Object>();
    	List<Object> data = new ArrayList<Object>();
    	//data.add("Empty Folder");    	
       	
    	RestTemplate restTemplate = new RestTemplate();
    	
    	restTemplate.getInterceptors().add(
    			  new BasicAuthorizationInterceptor("admin", "admin"));  	

    	data = callAtlas(server+api+"/v1/taxonomies/", restTemplate, data);
    	
    	dataHm.put("data", data);
    	coreHm.put("core", dataHm);   	
    	    	
    	return coreHm;
    }
    

    
	private static List<Object> callAtlas(String url, RestTemplate rt, List<Object> data) {
		// rt.setMessageConverters(Arrays.asList(new
		// MappingJackson2HttpMessageConverter()));
		ResponseEntity<List> response = rt.getForEntity(url, List.class);
		System.out.println(response.getBody());

		for (Object entry : response.getBody()) {
			Map e = (Map) entry;

			String urlTax = (String) e.get("href");
			HashMap<String, Object> taxfolder = new HashMap<String, Object>();
			String tax = (String) e.get("name");
			taxfolder.put("text", tax);
			data.add(taxfolder);
			//List<Object> children = new ArrayList<Object>();
			//folder.put("children", children);

			response = rt.getForEntity(urlTax, List.class);
			List<Object> termChildren = new ArrayList<Object>();
			HashMap<String, Object> prevTermFolder = new HashMap<String, Object>();
			String previousTerm = "_";
			for (Object taxonomy : response.getBody()) {
				Map ta = (Map) taxonomy;

				if (ta.containsKey("terms")) {
					String urlTerm = (String) ((Map) ta.get("terms")).get("href");

					response = rt.getForEntity(urlTerm, List.class);
					for (Object term : response.getBody()) {
						Map te = (Map) term;

						
						//HashMap<String, Object> termFolder = new HashMap<String, Object>();
						String termName = (String) te.get("name");
						HashMap<String, Object> termFolder = new HashMap<String, Object>();
						//termFolder.put("text", termName);
						
						
						if (termName.contains(previousTerm)) {
							//breadcrumb logic, flatten to hierarchy
							termFolder.put("text", termName.replace(previousTerm, "").replace(".", ""));
							List<Object> termSubChildren = new ArrayList<Object>();
							termSubChildren.add(termFolder);
							prevTermFolder.put("children", termSubChildren);
							
						}
						else {
							// add to parent tax folder
							termFolder.put("text", termName.replace(tax, "").replace(".", ""));
							termChildren.add(termFolder);
							taxfolder.put("children", termChildren);
						}
						
						prevTermFolder = termFolder;
						previousTerm = termName;
						
						
						String entityURL = server + api + "/v2/search/dsl?query=`" + (String) te.get("name") + "`"; // api/atlas/v2/search/dsl?query=`Catalog.Mining.PA.MineA.TruckA`
						System.out.println(entityURL);

						ResponseEntity<String> response2 = rt.getForEntity(entityURL, String.class);

						JSONParser parser = new JSONParser();
						JSONObject json = null;
						try {
							json = (JSONObject) parser.parse(response2.getBody());
						} catch (ParseException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}

						/*
						 * { "queryType": "DSL", "queryText":
						 * "`Catalog.Mining.PA.MineA.TruckA`", "entities": [ {
						 * "typeName": "historian_tag",
						 */
						System.out.println(response2.getBody());

						// Map json = (Map) response2.getBody();
						List<Map> entities = (List) json.get("entities");
						List<Object> entityChild = new ArrayList<Object>();
						for (Map ent : entities) {
							// System.out.println(ent);
							String typeName = (String) ent.get("typeName");
							if ("historian_tag".equalsIgnoreCase(typeName)) {
								String guid = (String) ent.get("guid");
								String displayText = (String) ent.get("displayText");
								System.out.println(displayText);

								
								
								HashMap<String, Object> entityfolder = new HashMap<String, Object>();
								entityfolder.put("text", displayText);
								
								
								entityChild.add(entityfolder);
								
								// add to parent folder
								termFolder.put("children", entityChild);
								
								

							}
						}
					}
				}
			}
		}

		return data;

	}
        
   
    
    
    private static List<Object> callAtlas2 (String url, RestTemplate rt, List<Object> data) {
    	//rt.setMessageConverters(Arrays.asList(new MappingJackson2HttpMessageConverter()));
        ResponseEntity<List> response = rt.getForEntity(url, List.class);
        System.out.println(response.getBody());
        
        for (Object entry :  response.getBody() ) {
        	Map e = (Map) entry;
        	//System.out.println(e +"  "+ e.get("href"));
        	if (e.containsKey("terms")) {
        		url = (String) ((Map) e.get("terms")).get("href");
        		callAtlas(url, rt, data);
        	}
        	else {
        		url = (String)e.get("href");
        		HashMap<String, Object> folder = new HashMap<String, Object>();
        		String term = (String) e.get("name");
        		folder.put("text", term);
        		
        		data.add(folder);
        		
        		List<Object> children = new ArrayList<Object>();
        		folder.put("children", children);
        		
        		if (!"Catalog".equalsIgnoreCase(term)) {

        		// for each term do the SQL search 
 
        		String entityURL = server+api+"/v2/search/dsl?query=`"+(String) e.get("name")+"`";  //api/atlas/v2/search/dsl?query=`Catalog.Mining.PA.MineA.TruckA`
    	    	System.out.println(entityURL);
    	    	
    	        ResponseEntity<String> response2 = rt.getForEntity(entityURL, String.class);
    	       
    	        JSONParser parser = new JSONParser();
    	        JSONObject json = null;
				try {
					json = (JSONObject) parser.parse(response2.getBody());
				} catch (ParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

    	       
           		/*{
    			"queryType": "DSL",
    			"queryText": "`Catalog.Mining.PA.MineA.TruckA`",
    			"entities": [
    			   {
    			"typeName": "historian_tag",
    		*/
    	        System.out.println(response2.getBody());
    	      
    	        //Map json = (Map) response2.getBody();
    	        List<Map> entities = (List) json.get("entities");
    	        for (Map ent: entities) {
    	        	//System.out.println(ent);
    	        	String typeName = (String) ent.get("typeName");
    	        	if ("historian_tag".equalsIgnoreCase(typeName)) {
    	        		String guid = (String) ent.get("guid");
    	        		String displayText = (String) ent.get("displayText");
    	        		System.out.println(displayText);
    	        		
    	        		
    	        		folder = new HashMap<String, Object>();
    	        		
    	        		folder.put("text", displayText);
    	        		
    	        		children.add(folder);
    	        		
    	        		
    	        		
    	        	}
    	        }
        		
        	}
        		
        		callAtlas(url, rt, children);
        	}
        		
        	
        }
        // need exit condition
		return data;
    }
    
}
