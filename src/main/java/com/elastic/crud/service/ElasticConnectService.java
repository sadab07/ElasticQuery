package com.elastic.crud.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpSession;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.client.indices.AnalyzeResponse.AnalyzeToken;
import org.springframework.http.ResponseEntity;

import com.elastic.crud.model.StringForJson;
import com.elastic.crud.model.Student;


public interface ElasticConnectService {
	public Map<String, Object> addStudent(String index,Student student,HttpSession httpSession);
	//public Map<String, Object> addStudents(String index, StringForJson stringForJson,HttpSession httpSession);
	
	SearchResponse getStudent(String index,HttpSession httpSession) throws IOException;
	Student getStudentById(String index,String id,HttpSession httpSession);
	public boolean deleteStudentById(String index,String id);
	public boolean deleteIndex(String index);
	public UpdateResponse updateStudentDetails(String index,String id,Student student,HttpSession httpSession) throws IOException;
	public void getMatchingStudent(String firstMatchingField, String firstMatchingValue,
			String secondMatchingField, String secondMatchingValue, HttpSession httpSession);
	public List<Student> matchSearch(HttpSession httpSession);

	public ResponseEntity<?> aggregationFunctions(String caseValue, String index);
	
	public ResponseEntity<?> cardinalityFunc();

	
	public ResponseEntity<?> percentile(String index);

	public ResponseEntity<?> topHits(String index);
	
	public ResponseEntity<?> filterFunc(String index);
	
	public ResponseEntity<?> DateAgg(String index) ;
	
	public ResponseEntity<?> Iprange (String index);
	
	public ResponseEntity<?> histogram (String index);
	
	public ResponseEntity<?> datehistogram (String index) ;
	
	public Map<String, Object> DemoinCreateMappingStatic(String text);
	
	public Map<String, Object> DemoinCreateFiledMapping(String text);
	
	public Map<String, Object> DemoinCreatetempalteStatic(String text);
	
	public ResponseEntity<?> geodistance (String index);

	ResponseEntity<?> scrollImplement() throws IOException;
	
	public ResponseEntity<?> scrollsImplement() throws IOException;
	
	public List<AnalyzeToken> Analyzer() throws IOException;
	
	public ResponseEntity<?> NestedAggregation(String index);
	
	public ResponseEntity<?> geogrid(String index) throws IOException;
}