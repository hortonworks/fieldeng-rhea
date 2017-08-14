package com.hortonworks.historian;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.hortonworks.historian.domain.AppProps;
import com.hortonworks.historian.model.Atlas;


@SpringBootApplication
public class HistorianWebApp {
	
	public static void main(String[] args) {
		ApplicationContext app = SpringApplication.run(HistorianWebApp.class, args);
		
		AppProps props = (AppProps) app.getBean(AppProps.class);
		System.out.println(props.getHostName());
		
		System.out.println(props.getHistorianApiHost());
		System.out.println(props.getHistorianApiPort());
		
		System.out.println(props.getAltasHost());
		System.out.println(props.getAltasPort());
		
		
		Atlas.init();
		
	}
}
