	package com.elastic.crud.config;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticConfig {

	public static RestHighLevelClient elasticSearchConn() {
		RestHighLevelClient client = null;
		try {
			/*
			 * esClient = new RestHighLevelClient( RestClient.builder(new
			 * HttpHost("10.1.3.25", 9200, "http")));
			 */ 
			
			final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
	        credentialsProvider.setCredentials(AuthScope.ANY,
	                new UsernamePasswordCredentials("elastic", "zeronsec@123"));

	        RestClientBuilder builder = RestClient.builder(new HttpHost("10.1.3.25", 9200, "http"))
	                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

	         client = new RestHighLevelClient(builder);


		}catch (Exception e) {
			e.printStackTrace();
		}
		return client;
	}
}
