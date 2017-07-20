package com.hortonworks.historian.controller;

import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
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
    public String  findPattern(
    		@RequestParam(value="tags") String tags, 
    		@RequestParam(value="sdate") String sdate, 
    		@RequestParam(value="edate") String edate , 
    		@RequestParam(value="function") String function,
    		@RequestParam(value="granularity") String granularity, 
    		@RequestParam(value="psdate") String pSDate, 
    		@RequestParam(value="pedate") String pEDate ) throws ParseException {
    	
    	
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
        	
        //{"text\/plain":"[[1497728580000,37.5],[1497728640000,31.04573631286621],[1497728700000,34.739479064941406],
	        	System.out.println(response);
        //}
	        	
        String[] dd = response.getBody().split(Pattern.quote("plain\":\""));
	      String array = 	dd[1].substring(0, dd[1].length() -5 ) + "]";
    	return array;
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
	        	objRow[1] = Double.parseDouble(cols[2]);
	        	tsList.add(objRow);
        	}
        	count++;
        	
        }
        }
        //System.out.println(response.getBody());
        return tsList;
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
    	/*
    	
    	HashMap<String, Object> coreHm = new HashMap<String, Object>();
    	HashMap<String, Object> dataHm = new HashMap<String, Object>();
    	List<Object> data = new ArrayList<Object>();
    	//data.add("Empty Folder");    	
       	
    	RestTemplate restTemplate = new RestTemplate();
    	
    	restTemplate.getInterceptors().add(
    			  new BasicAuthorizationInterceptor("admin", "admin"));  	

    	data = callAtlas("http://172.26.247.24:21000/api/atlas/v1/taxonomies/", restTemplate, data);
    	
    	dataHm.put("data", data);
    	coreHm.put("core", dataHm);   	
    	    	
    	return coreHm;
    	*/
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

    
    /*HashMap<String, Object> folderHm = new HashMap<String, Object>();
	folderHm.put("text", "resources");
	data.add(folderHm);
	
	HashMap<String, Object> openClose = new HashMap<String, Object>();
	openClose.put("opened", true);
	folderHm.put("state", openClose);
	
	List<Object> children = new ArrayList<Object>();
	folderHm.put("children", children);*/
}
