package com.elastic.crud.serviceImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.directory.SearchResult;
import javax.servlet.http.HttpSession;

import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.xml.builders.BooleanQueryBuilder;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import com.elastic.crud.config.ElasticConfig;
import com.elastic.crud.model.Student;
import com.elastic.crud.service.ElasticConnectService;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class ElasticConnectServiceImpl implements ElasticConnectService{
	
	@Autowired
	private ElasticConnectService elasticService;
	
	RestHighLevelClient client = ElasticConfig.elasticSearchConn();
	@Override
	public Map<String, Object> addStudent(String index,Student student,HttpSession httpSession) {
		/*
		 * For creating index and inserting a record
		 */
		
		IndexRequest request = new IndexRequest("jay");
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("age", student.getAge());
		map.put("gender", student.getGender());
		map.put("subject", student.getSubject());
		map.put("name", student.getName());
		httpSession.setAttribute("response", map);
		request.source(map, XContentType.JSON);
		
		try {
			IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
			System.out.println(indexResponse);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return map;
	}

	@Override
	public SearchResponse getStudent(String index,HttpSession httpSession) throws IOException {
		
		Map<String, Object> map = new HashMap<>();	
		/*
		 * For searching all details of index
		 * */
		SearchRequest searchRequestForAll = new SearchRequest(index);
		
		MatchAllQueryBuilder matchQueryBuilderForAll = QueryBuilders.matchAllQuery();
		SearchSourceBuilder sourceBuilderForAll = new SearchSourceBuilder();
		sourceBuilderForAll.query(matchQueryBuilderForAll);
		searchRequestForAll.source(sourceBuilderForAll);
		
		SearchResponse searchResponseForAll = client.search(searchRequestForAll,RequestOptions.DEFAULT);
		List list = new ArrayList();
		for(SearchHit s : searchResponseForAll.getHits().getHits())
		{
			 list.add(s.getSourceAsMap());
		}
		
		httpSession.setAttribute("res", list);
		map.put("response",list);
		map.put("code", 200);
		map.put("message", "Got response from elastic search's database successfully...");
		map.put("status", true);
		return searchResponseForAll;
	}

	@Override
	public Student getStudentById(String index,String id,HttpSession httpSession) {
		SearchRequest searchRequest = new SearchRequest(index);
		
		MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("_id", id);
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(matchQueryBuilder);
		searchRequest.source(sourceBuilder);
		List list = new ArrayList();
		try {
			SearchResponse searchResponse = client.search(searchRequest,RequestOptions.DEFAULT);
			for(SearchHit s : searchResponse.getHits().getHits())
			{
				list.add(s.getSourceAsMap());
			}
			httpSession.setAttribute("studentWithId", list);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;	
	}
	
	@Override
	public void getMatchingStudent(String firstMatchingField, String firstMatchingValue,
			String secondMatchingField, String secondMatchingValue, HttpSession httpSession) {
		
		MultiSearchRequest request = new MultiSearchRequest();
		
		SearchRequest firstSearchRequest = new SearchRequest();
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchQuery(firstMatchingField, firstMatchingValue));
		firstSearchRequest.source(searchSourceBuilder);
		request.add(firstSearchRequest);       
		
		SearchRequest secondSearchRequest = new SearchRequest();  
		searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchQuery(secondMatchingField, secondMatchingValue));
		secondSearchRequest.source(searchSourceBuilder);
		request.add(secondSearchRequest);
		
		try {
			MultiSearchResponse response = client.msearch(request, RequestOptions.DEFAULT);
			httpSession.setAttribute("matchingStudents", response);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}		
	}
	
	@Override
	public boolean deleteStudentById(String index,String id) {
		DeleteRequest deleteRequest = new DeleteRequest(index, id);
		try {
			DeleteResponse deleteResponseForIndexValue = client.delete(deleteRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public boolean deleteIndex(String index) {
		/*
		 * For deleting whole index
		 */
		DeleteIndexRequest requestForDelete = new DeleteIndexRequest(index);
		try {
			AcknowledgedResponse deleteResponse = client.indices().delete(requestForDelete, RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public UpdateResponse updateStudentDetails(String index, String id,Student student,HttpSession httpSession) throws IOException {
		/*
	       * For updating index value commented because value will update when we run the program everytime...
	       * */
	      UpdateRequest updateRequest = new UpdateRequest(index,id);
			ObjectMapper om = new ObjectMapper();
			String jsonString = om.writeValueAsString(student);
	    	updateRequest.doc(jsonString, XContentType.JSON);
	    	Map<String, Object> map = new HashMap<String, Object>();
	    	map.put("name", student.getName());
	    	map.put("age", student.getAge());
	    	map.put("gender", student.getGender());
	    	map.put("subject", student.getSubject());
	    	httpSession.setAttribute("updateRes", map);
	    	UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
			return updateResponse;
	}

	public List<Student> matchSearch(HttpSession httpSession) {
		MultiSearchRequest request = new MultiSearchRequest();
		
		SearchRequest firstSearchRequest = new SearchRequest("jay");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("name", "JAY_SADAB")));

	//	searchSourceBuilder.query(QueryBuilders.boolQuery().mustNot(QueryBuilders.matchQuery("name", "Akash")));
	//	searchSourceBuilder.query(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("age", 20)));
//		searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("age").gt(22)));
	//	searchSourceBuilder.query(QueryBuilders.boolQuery().filter(QueryBuilders.matchQuery("age",22)));
//		searchSourceBuilder.query(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("age", 20)));
//		searchSourceBuilder.query(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("age", 20)));
		firstSearchRequest.source(searchSourceBuilder);
		request.add(firstSearchRequest);
		try {
			
			MultiSearchResponse response = client.msearch(request, RequestOptions.DEFAULT);
			MultiSearchResponse.Item firstResponse = response.getResponses()[0];                               
			SearchResponse searchResponse = firstResponse.getResponse();           
			MultiSearchResponse.Item secondResponse = response.getResponses()[0];  
			searchResponse = secondResponse.getResponse();
			List list = new ArrayList();
			
			for(SearchHit s : searchResponse.getHits().getHits())
			{
				list.add(s.getSourceAsMap());
			}
			httpSession.setAttribute("boolQueryResult", list);
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return null;		
	}
}