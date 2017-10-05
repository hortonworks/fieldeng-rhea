package com.hortonworks.historian.controller;

import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.support.BasicAuthorizationInterceptor;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.google.common.collect.Lists;
import com.hortonworks.historian.model.Atlas;

@Component
@RestController
public class Controller{
	static final Logger LOG = LoggerFactory.getLogger(Controller.class);
	
	@Value("${historian.api.host}")
	private String historianApiHost;
	
	@Value("${historian.api.port}")
	private String historianApiPort;
	
	//@Value("${kafka.host}")
	private String kafkaHost;
		
	//@Value("${kafka.port}")
	private String kafkaPort;
		
	//@Value("${zk.host}")
	private String zkHost;
		
	//@Value("${zk.port}")
	private String zkPort;
	
	private String server = "http://"+historianApiHost+":"+historianApiPort;
	
	public Controller() {
		Map<String, String> env = System.getenv();
        if(env.get("API_HOST") != null){
        	historianApiHost = (String)env.get("API_HOST");
        }
        if(env.get("API_PORT") != null){
        	historianApiPort = (String)env.get("API_PORT");
        }
        
        server = "http://"+historianApiHost+":"+historianApiPort;
        System.out.println("********************** Controller ()  API url set tp: " + server);
	}
	
    @RequestMapping(value="/search", produces = { MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<List<String>> search(@RequestParam(value="term") String text) {
    	
    	List<String> searchResults = new ArrayList<String>();

    	Lists.newArrayList("cat","mouse","dog");
    	LOG.trace(text);

    	for (String name: Lists.newArrayList("cat","mouse","dog") ) {
    		if (name.toLowerCase().contains(text.toLowerCase())) {
	    		LOG.debug("match: "+ name);
	    		searchResults.add(name);
    		}
    	}
    	
    	if (!searchResults.isEmpty()) {
    		return new ResponseEntity<List<String>>(searchResults, HttpStatus.OK);
    	}
    	else {
    		return new ResponseEntity<List<String>>(searchResults, HttpStatus.BAD_REQUEST);
    	}	
    }
	

    @RequestMapping(value="/test1", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public HashMap<String, Object> tablestats() {

    	
    	HashMap<String, Object> coreHm = new HashMap<String, Object>();
    	HashMap<String, Object> dataHm = new HashMap<String, Object>();
    	List<Object> data = new ArrayList<Object>();
    	data.add("Empty Folder");
    	HashMap<String, Object> folderHm = new HashMap<String, Object>();
    	folderHm.put("text", "resources");
    	data.add(folderHm);
    	
    	HashMap<String, Object> openClose = new HashMap<String, Object>();
    	openClose.put("opened", true);
    	folderHm.put("state", openClose);
    	
    	List<Object> children = new ArrayList<Object>();
    	folderHm.put("children", children);
    	
    	coreHm.put("core", dataHm);
    	dataHm.put("data", data);
    	
    	
    	return coreHm;
    }
    
    
    
    @RequestMapping(value="/gettimeseries", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public List<Object[]>  getTimeSeries() throws ParseException {
    	
    	List<Object[]> tsList = new ArrayList<Object[]>();
    	    	
    	RestTemplate rt = new RestTemplate();
    	
    	rt.getInterceptors().add(
    			  new BasicAuthorizationInterceptor("admin", "admin"));  	

        ResponseEntity<String> response = rt.getForEntity(server + "/?tags=mpg_truck_a&granularity=minute&function=sum&sdate=2017-04-23_00:00:00&edate=2017-05-03_00:00:00",
        		String.class);
        
        String[] ds = response.getBody().split(Pattern.quote("\n"));
        int count =0;
        for (String row : ds) {
        	if (count > 0) {
        		Object[] objRow = new Object[2];	
        		
	        	System.out.println(row);
	        	String[] cols = row.split(Pattern.quote(","));	        	
	        	Date d  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(cols[0]);
	        	objRow[0] = d.getTime();
	        	objRow[1] = Double.parseDouble(cols[1]);
	        	tsList.add(objRow);
        	}
        	count++;
        	
        }
        
        System.out.println(response.getBody());
        
        return tsList;
    }
  
    
    @RequestMapping(value="/getmovavg", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public List<Object[]>  getMovAvg() throws ParseException {
    	
    	List<Object[]> tsList = new ArrayList<Object[]>();
    	    	
    	RestTemplate rt = new RestTemplate();
    	
    	rt.getInterceptors().add(
    			  new BasicAuthorizationInterceptor("admin", "admin"));  	

        ResponseEntity<String> response = rt.getForEntity(
        		server+"/aggregate?tags=mpg_truck_a&granularity=minute&function=mov_avg&sdate=2017-04-23_00:00:00&edate=2017-05-03_00:00:00",
        		String.class);
        
        String[] ds = response.getBody().split(Pattern.quote("\n"));
        int count =0;
        for (String row : ds) {
        	if (count > 0) {
        		Object[] objRow = new Object[2];	
        		
	        	System.out.println(row);
	        	String[] cols = row.split(Pattern.quote(","));	        	
	        	Date d  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(cols[0]);
	        	objRow[0] = d.getTime();
	        	objRow[1] = Double.parseDouble(cols[1]);
	        	tsList.add(objRow);
        	}
        	count++;
        }
        
        System.out.println(response.getBody());
        
        return tsList;
    }

    
    @RequestMapping(value="/getmax", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public List<Object[]>  getMax() throws ParseException {
    	
    	List<Object[]> tsList = new ArrayList<Object[]>();
    	    	
    	RestTemplate rt = new RestTemplate();
    	
    	rt.getInterceptors().add(
    			  new BasicAuthorizationInterceptor("admin", "admin"));  	

        ResponseEntity<String> response = rt.getForEntity(
        		server+"/?tags=mpg_truck_a&granularity=minute&function=max&sdate=2017-04-23_00:00:00&edate=2017-05-03_00:00:00",
        		String.class);
        
        String[] ds = response.getBody().split(Pattern.quote("\n"));
        int count =0;
        for (String row : ds) {
        	if (count > 0) {
        		Object[] objRow = new Object[2];	
        		
	        	System.out.println(row);
	        	String[] cols = row.split(Pattern.quote(","));	        	
	        	Date d  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(cols[0]);
	        	objRow[0] = d.getTime();
	        	objRow[1] = Double.parseDouble(cols[1]);
	        	tsList.add(objRow);
        	}
        	count++;
        	
        }
        
        System.out.println(response.getBody());
        
        return tsList;
    }
    
    
    @RequestMapping(value="/getmin", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public List<Object[]>  getMin() throws ParseException {
    	
    	List<Object[]> tsList = new ArrayList<Object[]>();
    	    	
    	RestTemplate rt = new RestTemplate();
    	
    	rt.getInterceptors().add(
    			  new BasicAuthorizationInterceptor("admin", "admin"));  	

        ResponseEntity<String> response = rt.getForEntity(
        		server+"/?tags=mpg_truck_a&granularity=minute&function=min&sdate=2017-04-23_00:00:00&edate=2017-05-03_00:00:00",
        		String.class);
        
        if (response.getBody() != null && !response.getBody().isEmpty()) {
        
        String[] ds = response.getBody().split(Pattern.quote("\n"));
        int count =0;
        for (String row : ds) {
        	if (count > 0) {
        		Object[] objRow = new Object[2];	
        		
	        	System.out.println(row);
	        	String[] cols = row.split(Pattern.quote(","));	        	
	        	Date d  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(cols[0]);
	        	objRow[0] = d.getTime();
	        	objRow[1] = Double.parseDouble(cols[1]);
	        	tsList.add(objRow);
        	}
        	count++;
        	
        }
        }
        
        System.out.println(response.getBody());
        
        return tsList;
    }
    
    //http://localhost:8080/findpattern?tags=mpg_truck_a&sdate=2017-04-23_00:00:00&edate=2017-05-03_00:00:00&function=sum&granularity=minute&psdate=1493096940000&pedate=1493099990590.072
    @RequestMapping(value="/findpattern", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public List<List<List>>  findPattern(
    		@RequestParam(value="tags") String tags, 
    		@RequestParam(value="sdate") String sdate, 
    		@RequestParam(value="edate") String edate , 
    		@RequestParam(value="function") String function,
    		@RequestParam(value="granularity") String granularity, 
    		@RequestParam(value="psdate") String pSDate, 
    		@RequestParam(value="pedate") String pEDate ) throws ParseException {
    	
    	List<List<List>> allMatches = new ArrayList();
    	
    	String millisS ="";
    	String millisE = "";
    	if (pSDate != null && pSDate.contains(".")) {
    		millisS = pSDate.split(Pattern.quote("."))[0];
    	}
    	
    	if (pEDate != null && pEDate.contains(".")) {
    		 millisE = pEDate.split(Pattern.quote("."))[0];
    	}
    	Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(Long.parseLong(millisE));
    	
    	
    	TimeZone tzUTC = TimeZone.getTimeZone( "UTC" );
    	TimeZone.getTimeZone("UTC");
    	
    	
    	cal.add(Calendar.HOUR, -4);
    	
    	String pEDateStr = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(cal.getTime() );
    	
    	  cal.setTimeInMillis(Long.parseLong(millisS));
    	  cal.add(Calendar.HOUR, -4);
    	
    	String pSDateStr = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(cal.getTime() );
    	
    	RestTemplate rt = new RestTemplate();

    	rt.getInterceptors().add(
    			  new BasicAuthorizationInterceptor("admin", "admin"));  	

    	
    	if (sdate.isEmpty()) {
    		Calendar cal1 = Calendar.getInstance();
    		cal1.add(Calendar.DATE, -13);
    		sdate = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(cal1.getTime());
    	}
    	else {
        	Date sd = new SimpleDateFormat("MM/dd/yyyy").parse(sdate);
    		sdate = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(sd);
    	}
    	if (edate.isEmpty()) {
    		edate = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(new Date());
    	}
    	else {
    		Date ed = new SimpleDateFormat("MM/dd/yyyy").parse(edate);
    		edate = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(ed);
    	}
    	
    	
    	String urlTemplate = server+"/spark/scala/pattern?tags={0}&granularity={1}&function={2}&sdate={3}&edate={4}&psdate={5}&pedate={6}";
    	String url = MessageFormat.format(urlTemplate, tags, granularity, function, sdate, edate, pSDateStr, pEDateStr);
    	
    	System.out.println(url);
        ResponseEntity<String> response = rt.getForEntity(url, String.class);
        
      //  String[] ds = response.getBody().split(Pattern.quote("********************"));
        //for (String row : ds) {
        	
        // [[1497728580000,37.5],[1497728640000,31.04573631286621],[1497728700000,34.739479064941406]]
	        	System.out.println(response);
        //}
	        	
	    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	    //2017-07-25 00:20:00.0,65.25
	        	
        String[] dd = response.getBody().split(Pattern.quote("plain\":\"["));
        String[] matchesArr = dd[1].split(Pattern.quote("],"));
       
        for (String match : matchesArr) {
        	String[] cols = match.split(Pattern.quote("|"));
        	for (String col : cols) {
        		List<List> matches = new ArrayList<List>(); 	
        		String[] colArr = col.split(Pattern.quote("?"));
        		
        		for (String c : colArr) {
        			
        		String values[] = c.split(Pattern.quote(","));
        		if (values != null && values.length >= 2) {
        			List l = new ArrayList();
        			l.add(df.parse(values[1]).getTime());
                	l.add(Double.parseDouble(values[2].replace(")", "") .replace("]", "") ));
                	matches.add(l);
        		}
        		
        		}
        		System.out.println(col);
        		allMatches.add(matches);
        	}	
        }
    	return allMatches;
    }
 
    
    @RequestMapping(value="/getts", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public List<Object[]>  getTs(
    		@RequestParam(value="tags") String tags, 
    		@RequestParam(value="sdate") String sdate, 
    		@RequestParam(value="edate") String edate , 
    		@RequestParam(value="function") String function,
    		@RequestParam(value="granularity") String granularity) throws ParseException {
    	
    	
    	if (sdate.isEmpty()) {
    		Calendar cal = Calendar.getInstance();
    		cal.add(Calendar.DATE, -13);
    		sdate = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(cal.getTime());
    	}
    	else {
        	Date sd = new SimpleDateFormat("MM/dd/yyyy").parse(sdate);
    		sdate = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(sd);
    	}
    	if (edate.isEmpty()) {
    		edate = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(new Date());
    	}
    	else {
    		Date ed = new SimpleDateFormat("MM/dd/yyyy").parse(edate);
    		edate = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(ed);
    	}
    			
    	List<Object[]> tsList = new ArrayList<Object[]>();
    	RestTemplate rt = new RestTemplate();
    	
    	rt.getInterceptors().add(
    			  new BasicAuthorizationInterceptor("admin", "admin"));  	
    	//mpg_truck_a
    	String urlTemplate = server+"/hive/aggregate?tags={0}&granularity={1}&function={2}&sdate={3}&edate={4}";
    	String url = MessageFormat.format(urlTemplate, tags, granularity, function, sdate, edate);
    			System.out.println(url);
        ResponseEntity<String> response = rt.getForEntity(url, String.class);
        
        if (response.getBody() != null && !response.getBody().isEmpty()) {
        	String[] ds = response.getBody().split(Pattern.quote("\n"));
        	int count =0;
        	for (String row : ds) {
        		if (count > 0) {
        			Object[] objRow = new Object[2];	
        		
        			System.out.println(row);
        			String[] cols = row.split(Pattern.quote(","));	        	
        			Date d  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(cols[0]);
        			objRow[0] = d.getTime();
        			objRow[1] = Double.parseDouble(cols[1]);
        			tsList.add(objRow);
        		}
        		count++;
        	}
        }
        //System.out.println(response.getBody());
        return tsList;
    }
    
    private List<Map<String, Object>> updateFileTree(String entityName, String entityType, String fullPath, String operationType, String targetLeaf, List<Map<String,Object>> currentNode){
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
					entity.put("type", entityType);
					if(currentLeaf.containsKey("children")){
						updatedNode = ((List<Map<String,Object>>)currentLeaf.get("children"));
					}else{
						currentLeaf.put("children", new ArrayList<Map<String,Object>>());
						updatedNode = ((List<Map<String,Object>>)currentLeaf.get("children"));
					}
					System.out.println(entity);
					if(operationType.equalsIgnoreCase("add")){
						updatedNode.add(entity);
					}else if(operationType.equalsIgnoreCase("remove")){
						updatedNode.remove(updatedNode.indexOf(entity));
					}
					return updatedNode;
				}else{
					targetLeaf = targetLeaf.split(currentBranch+".")[1];
					System.out.println(targetLeaf);
					List<Map<String,Object>> nextLeaf = (List<Map<String,Object>>)currentLeaf.get("children");
					updatedNode = updateFileTree(entityName, entityType, fullPath, operationType, targetLeaf, nextLeaf);	
				}
			}
		}
		return updatedNode;
	}
    
    @RequestMapping(value="/getpattern", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public List<Object[]>  getPattern(
    		@RequestParam(value="tags") String tags, 
    		@RequestParam(value="sdate") String sdate, 
    		@RequestParam(value="edate") String edate , 
    		@RequestParam(value="function") String function,
    		@RequestParam(value="granularity") String granularity,
    		@RequestParam(value="psdate") String psdate,
    		@RequestParam(value="pedate") String pedate
    		) throws ParseException {
    	
    	
    	if (sdate.isEmpty()) {
    		Calendar cal = Calendar.getInstance();
    		cal.add(Calendar.DATE, -10);
    		sdate = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(cal.getTime());
    	}
    	if (edate.isEmpty()) {
    		edate = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(new Date());
    	}
    	
    	List<Object[]> tsList = new ArrayList<Object[]>();
    	RestTemplate rt = new RestTemplate();
    	
    	rt.getInterceptors().add(
    			  new BasicAuthorizationInterceptor("admin", "admin"));  	
    	//mpg_truck_a
    	String urlTemplate = server+"/pattern?tags={0}&granularity={1}&function={2}&sdate={3}&edate={4}&psdate={5}&pedate={6}";
    	String url = MessageFormat.format(urlTemplate, tags, granularity, function, sdate, edate, psdate, pedate);
    			
        ResponseEntity<String> response = rt.getForEntity(url, String.class);
        
        if (response.getBody() != null && !response.getBody().isEmpty()) {
        String[] ds = response.getBody().split(Pattern.quote("\n"));
        int count =0;
        for (String row : ds) {
        	if (count > 0) {
        		Object[] objRow = new Object[2];	
        		
	        	System.out.println(row);
	        	String[] cols = row.split(Pattern.quote(","));	        	
	        	Date d  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(cols[0]);
	        	objRow[0] = d.getTime();
	        	objRow[1] = Double.parseDouble(cols[1]);
	        	tsList.add(objRow);
        	}
        	count++;
        	
        }
        }
        //System.out.println(response.getBody());
        return tsList;
    }
 
    
    @RequestMapping(value="/getvalue", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public List<Object[]>  getValue() throws ParseException {
    	
    	List<Object[]> tsList = new ArrayList<Object[]>();
    	    	
    	RestTemplate rt = new RestTemplate();
    	
    	rt.getInterceptors().add(
    			  new BasicAuthorizationInterceptor("admin", "admin"));  	

        ResponseEntity<String> response = rt.getForEntity(
        		 "http://historian01-381-1-0.field.hortonworks.com:8055/?tags=mpg_truck_a&granularity=none&function=sum&sdate=2017-04-23_00:00:00&edate=2017-05-03_00:00:00",
        		String.class);
        
        String[] ds = response.getBody().split(Pattern.quote("\n"));
        int count =0;
        for (String row : ds) {
        	if (count > 0) {
        		Object[] objRow = new Object[2];	
        		
	        	System.out.println(row);
	        	String[] cols = row.split(Pattern.quote(","));	        	
	        	Date d  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(cols[0]);
	        	objRow[0] = d.getTime();
	        	objRow[1] = Double.parseDouble(cols[1]);
	        	tsList.add(objRow);
        	}
        	count++;
        }
        
        System.out.println(response.getBody());
        
        return tsList;
    }

    @RequestMapping(value="/getfiletree", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public HashMap<String, Object>  getFileTree() {
    
    	return Atlas.atlasFileTree;
    }
    
    @RequestMapping(value="/createTerm", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public void createTerm(@RequestParam("path") String targetLeaf, @RequestParam("termName") String termName) {
    	Atlas.createTerm(targetLeaf.replaceAll("\\.","/terms/"), termName);
    	List<Map<String,Object>> currentNode = (List<Map<String, Object>>) ((HashMap)Atlas.atlasFileTree.get("core")).get("data");
    	String fullPath = targetLeaf;
    	targetLeaf = String.join(".", Arrays.copyOf(targetLeaf.split("\\."), targetLeaf.split("\\.").length-1));
    	System.out.println(termName);
    	System.out.println(targetLeaf);
    	System.out.println(currentNode);
    	updateFileTree(termName, "folder", fullPath, "add", targetLeaf, currentNode);
    }
    
    @RequestMapping(value="/deleteTerm", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public void deleteTerm(@RequestParam("path") String targetLeaf, @RequestParam("termName") String termName) {
    	Atlas.deleteTerm(targetLeaf.replaceAll("\\.","/terms/"), termName);
    	List<Map<String,Object>> currentNode = (List<Map<String, Object>>) ((HashMap)Atlas.atlasFileTree.get("core")).get("data");
    	String fullPath = targetLeaf;
    	targetLeaf = String.join(".", Arrays.copyOf(targetLeaf.split("\\."), targetLeaf.split("\\.").length-1));
    	System.out.println(termName);
    	System.out.println(targetLeaf);
    	System.out.println(currentNode);
    	updateFileTree(termName, "folder", fullPath, "remove", targetLeaf, currentNode);
    }
    
    @RequestMapping(value="/moveNode", method=RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE})
    public void moveNode(@RequestParam("nodeName") String nodeName, @RequestParam("type") String type, @RequestParam("oldPath") String oldPath, @RequestParam("newPath") String newPath) {
    	String targetLeaf = oldPath; 
    	if(type.equalsIgnoreCase("tag")){
    		Atlas.removeTerm(targetLeaf, nodeName);
    	}else if(type.equalsIgnoreCase("folder")){
    		Atlas.deleteTerm(targetLeaf.replaceAll("\\.","/terms/"), nodeName);
    	}
    	List<Map<String,Object>> currentNode = (List<Map<String, Object>>) ((HashMap)Atlas.atlasFileTree.get("core")).get("data");
    	String fullPath = targetLeaf+"."+nodeName;
    	//targetLeaf = String.join(".", Arrays.copyOf(targetLeaf.split("\\."), targetLeaf.split("\\.").length-1));
    	System.out.println(nodeName);
    	System.out.println(targetLeaf);
    	System.out.println(currentNode);
    	updateFileTree(nodeName, type, fullPath, "remove", targetLeaf, currentNode);
    	
    	targetLeaf = newPath; 
    	if(type.equalsIgnoreCase("tag")){
    		Atlas.applyTerm(targetLeaf, nodeName);
    	}else if(type.equalsIgnoreCase("folder")){
    		Atlas.createTerm(targetLeaf.replaceAll("\\.","/terms/"), nodeName);
    	}
    	currentNode = (List<Map<String, Object>>) ((HashMap)Atlas.atlasFileTree.get("core")).get("data");
    	fullPath = targetLeaf+"."+nodeName;
    	//targetLeaf = String.join(".", Arrays.copyOf(targetLeaf.split("\\."), targetLeaf.split("\\.").length-1));
    	System.out.println(nodeName);
    	System.out.println(targetLeaf);
    	System.out.println(currentNode);
    	updateFileTree(nodeName, type, fullPath, "add", targetLeaf, currentNode);
    }
    
    private List<Object> callAtlas (String url, RestTemplate rt, List<Object> data) {
    	rt.setMessageConverters(Arrays.asList(new MappingJackson2HttpMessageConverter()));
        ResponseEntity<List> response = rt.getForEntity(url, List.class);
        System.out.println(response.getBody());
        
        for (Object entry :  response.getBody() ) {
        	Map e = (Map) entry;
        	//System.out.println(e +"  "+ e.get("href"));
        	if (e.containsKey("terms")) {
        		url = (String) ((Map) e.get("terms")).get("href");
        		
        	}
        	else {
        		url = (String)e.get("href");
        		HashMap<String, Object> folderHm = new HashMap<String, Object>();
        		folderHm.put("text", (String) e.get("name"));
        		data.add(folderHm);
        	}
        		callAtlas(url, rt, data);
        	
        }
        // need exit condition
		return data;
    }
}
