package com.hortonworks.historian.model;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.commons.net.util.Base64;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.support.BasicAuthorizationInterceptor;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import scala.util.parsing.json.JSON;

public abstract class Atlas {

	public static List<AtlasItem> atlasCache = new ArrayList<AtlasItem>();
	public static String atlasHost = "localhost";
	public static String atlasPort = "21000";
	public static String server = "http://"+atlasHost+":"+atlasPort;
	//public static String server = "http://historian03-499-3-2.field.hortonworks.com:21000";
	public static String api = "/api/atlas";
	//http://historian01-381-3-2:21000/api/atlas/v2/search/dsl?query=`Catalog.Mining.PA.MineA.TruckA`
	
	private static AtlasClient atlasClient;
	
	public static HashMap<String, Object> atlasFileTree = new HashMap<String, Object>();
	public static HashMap<String, Object> tagsTaxMapping = new HashMap<String, Object>();
	
	private static String ATLAS_USER = "admin";
	private static String ATLAS_PASSWORD = "admin";
	private static String[] basicAuth = {ATLAS_USER, ATLAS_PASSWORD};
	
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
        
        String[] atlasURL = {server};
		
    	if (atlasClient == null) {
           System.out.println("Creating new Atlas client for" + server);
            atlasClient = new AtlasClient(atlasURL, basicAuth);
        }
        
		atlasFileTree = getFileTree();
		System.out.println("********* FileTree: " + atlasFileTree);
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
    	dataHm.put("check_callback","true");
    	coreHm.put("core", dataHm);   	
    	    	
    	return coreHm;
    }
    
    public static void createTerm(String taxonomyPath, String termName) {
    	
    	System.out.println("********** " + taxonomyPath);
    	RestTemplate restTemplate = new RestTemplate();
    	restTemplate.getInterceptors().add(new BasicAuthorizationInterceptor("admin", "admin")); 
    	String url = server+api+"/v1/taxonomies/";
    	System.out.println("********** " + url+taxonomyPath);
    	String termDefinition = "{\"name\":\""+termName+"\",\"description\":\"\"}";
    	
    	HttpHeaders headers = new HttpHeaders();
    	headers.setContentType(MediaType.APPLICATION_JSON);
    	MultiValueMap<String, String> map= new LinkedMultiValueMap<String, String>();
    	map.add("name", termName);
    	map.add("description", "");

    	HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<MultiValueMap<String, String>>(map, headers);
    	//return restTemplate.postForEntity(url+taxonomyPath, request, String.class).toString();
    	try {
			postJSONToUrlAuth(url+taxonomyPath, basicAuth, termDefinition);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
    }
    
    public static void deleteTerm(String taxonomyPath, String termName) {
    	System.out.println("********** " + taxonomyPath);
    	RestTemplate restTemplate = new RestTemplate();
    	restTemplate.getInterceptors().add(new BasicAuthorizationInterceptor("admin", "admin")); 
    	String url = server+api+"/v1/taxonomies/";
    	
    	HttpHeaders headers = new HttpHeaders();
    	headers.setContentType(MediaType.APPLICATION_JSON);

    	//HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<MultiValueMap<String, String>>(map, headers);
    	//return restTemplate.postForEntity(url+taxonomyPath, request, String.class).toString();
    	try {
			deleteUrlAuth(url+taxonomyPath, basicAuth);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
    }
    
    public static void applyTerm(String termName, String entityName) {
    	try {
    		JSONArray result = atlasClient.searchByDSL("historian_tag where name='"+entityName+"'", 1, 0);
    		System.out.println("********** applyTerm() found entity: " + result);
    		String guid = result.getJSONObject(0).getJSONObject("$id$").getString("id");
        	String url = server+api+"/v1/entities/"+guid+"/tags/"+termName;
			postJSONToUrlAuth(url, basicAuth, "{}");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (AtlasServiceException e) {
			e.printStackTrace();
		}
    }
    
    public static void removeTerm(String termName, String entityName) {
    	try {
    		JSONArray result = atlasClient.searchByDSL("historian_tag where name='"+entityName+"'", 1, 0);
    		System.out.println("********** removeTerm() found entity: " + result);
    		String guid = result.getJSONObject(0).getJSONObject("$id$").getString("id");
        	String url = server+api+"/v1/entities/"+guid+"/tags/"+termName;
			deleteUrlAuth(url, basicAuth);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (AtlasServiceException e) {
			e.printStackTrace();
		}
    }
    
    private static JSONObject postJSONToUrlAuth(String urlString, String[] basicAuth, String payload) throws IOException, JSONException {
		String userPassString = basicAuth[0]+":"+basicAuth[1];
		JSONObject json = null;
		try {
            URL url = new URL (urlString);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("Authorization", "Basic " + new String(Base64.encodeBase64(userPassString.getBytes())));
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("X-XSRF-HEADER","User");
            
            OutputStream os = connection.getOutputStream();
    		os.write(payload.getBytes());
    		os.flush();
            
            if (connection.getResponseCode() > 202) {
    			throw new RuntimeException("Failed : HTTP error code : " + connection.getResponseCode());
    		}
            BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream(), Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	json = new JSONObject();
        } catch(Exception e) {
            e.printStackTrace();
        }
        return json;
    }
    
    private static void deleteUrlAuth(String urlString, String[] basicAuth) throws IOException, JSONException {
		String userPassString = basicAuth[0]+":"+basicAuth[1];
		JSONObject json = null;
		try {
            URL url = new URL (urlString);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("DELETE");
            connection.setDoOutput(true);
            connection.setRequestProperty("Authorization", "Basic " + new String(Base64.encodeBase64(userPassString.getBytes())));
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("X-XSRF-HEADER","User");
            
            InputStream content = (InputStream)connection.getInputStream();
            if (connection.getResponseCode() > 202) {
    			throw new RuntimeException("Failed : HTTP error code : " + connection.getResponseCode());
    		}
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
	
	private static String readAll(Reader rd) throws IOException {
	    StringBuilder sb = new StringBuilder();
	    int cp;
	    while ((cp = rd.read()) != -1) {
	      sb.append((char) cp);
	    }
	    return sb.toString();
	}
    
	private static List<Object> callAtlas(String url, RestTemplate rt, List<Object> data) {
		// rt.setMessageConverters(Arrays.asList(new
		// MappingJackson2HttpMessageConverter()));
		ResponseEntity<List> response = rt.getForEntity(url, List.class);
		System.out.println("********** Top: "+response.getBody());

		for (Object entry : response.getBody()) {
			Map e = (Map) entry;

			String urlTax = (String) e.get("href");
			HashMap<String, Object> taxfolder = new HashMap<String, Object>();
			String tax = (String) e.get("name");
			taxfolder.put("id", tax);
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
						System.out.println("********** term: " + term);
						
						//HashMap<String, Object> termFolder = new HashMap<String, Object>();
						String termName = (String) te.get("name");
						HashMap<String, Object> termFolder = new HashMap<String, Object>();
						//termFolder.put("text", termName);
						
						
						if (termName.contains(previousTerm)) {
							//breadcrumb logic, flatten to hierarchy
							termFolder.put("id", termName);
							termFolder.put("type", "folder");
							termFolder.put("text", termName.replace(previousTerm, "").replace(".", ""));
							List<Object> termSubChildren = new ArrayList<Object>();
							termSubChildren.add(termFolder);
							prevTermFolder.put("children", termSubChildren);
							
						}
						else {
							// add to parent tax folder
							termFolder.put("id", termName);
							termFolder.put("type", "folder");
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
								
								//String currentTermName = (String) te.get("name");
								
								String tagName = (String) ent.get("displayText");
								if(tagsTaxMapping.containsKey(tagName)){
									List<String> refContainers = (ArrayList<String>) tagsTaxMapping.get(tagName);
									refContainers.add(termName);
									tagsTaxMapping.put(tagName, refContainers);
								}else{
									List<String> containers = new ArrayList<String>();
									containers.add(termName);
									tagsTaxMapping.put(tagName, containers);
								}
								
								HashMap<String, Object> entityfolder = new HashMap<String, Object>();
								entityfolder.put("id", termName+"."+tagName);
								entityfolder.put("text", displayText);
								entityfolder.put("type", "tag");
								
								
								entityChild.add(entityfolder);
								
								// add to parent folder
								termFolder.put("children", entityChild);
							}
						}
					}
				}
			}
		}
		System.out.println("********** taxonomy data:" +data);
		System.out.println("********** Tags to Classificaiton Mappings: " +tagsTaxMapping);
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
