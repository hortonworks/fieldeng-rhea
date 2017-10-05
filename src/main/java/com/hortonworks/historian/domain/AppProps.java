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


	public String getHistorianApiPort() {
		return historianApiPort;
	}


	public void setHistorianApiPort(String historianApiPort) {
		this.historianApiPort = historianApiPort;
	}


	public String getHistorianApiHost() {
		return historianApiHost;
	}


	public void setHistorianApiHost(String historianApiHost) {
		this.historianApiHost = historianApiHost;
	}


	public String getKafkaPort() {
		return kafkaPort;
	}


	public void setKafkaPort(String kafkaPort) {
		this.kafkaPort = kafkaPort;
	}


	public String getKafkaHost() {
		return kafkaHost;
	}


	public void setKafkaHost(String kafkaHost) {
		this.kafkaHost = kafkaHost;
	}


	public String getZkHost() {
		return zkHost;
	}


	public void setZkHost(String zkHost) {
		this.zkHost = zkHost;
	}


	public String getZkPort() {
		return zkPort;
	}


	public void setZkPort(String zkPort) {
		this.zkPort = zkPort;
	}
	
}
