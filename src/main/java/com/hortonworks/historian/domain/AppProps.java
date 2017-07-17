package com.hortonworks.historian.domain;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AppProps {

	@Value("${server.port}")
	private String serverPort;

	@Value("${host.name}")
	private String hostName;
	
	@Value("${atlas.port}")
	private String altasPort;
	
	@Value("${atlas.host}")
	private String altasHost;
	
	@Value("${atlas.api}")
	private String altasApi;

	
	 public String getAltasPort() {
		return altasPort;
	}


	public void setAltasPort(String altasPort) {
		this.altasPort = altasPort;
	}


	public String getAltasHost() {
		return altasHost;
	}


	public void setAltasHost(String altasHost) {
		this.altasHost = altasHost;
	}


	public String getAltasApi() {
		return altasApi;
	}


	public void setAltasApi(String altasApi) {
		this.altasApi = altasApi;
	}


	public String getServerPort() {
		return serverPort;
	}


	public void setServerPort(String serverPort) {
		this.serverPort = serverPort;
	}


	public String getHostName() {
		return hostName;
	}


	public void setHostName(String hostName) {
		this.hostName = hostName;
	}


	public void print() {
		    System.out.println(hostName);
		  }
	
}
