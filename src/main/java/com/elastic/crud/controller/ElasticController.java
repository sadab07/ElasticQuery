package com.elastic.crud.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpSession;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.elastic.crud.model.StringForJson;
import com.elastic.crud.model.Student;
import com.elastic.crud.service.ElasticConnectService;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Controller
@RequestMapping("/student")
public class ElasticController {

	@Autowired
	ElasticConnectService elasticService;

	@PostMapping("/enroll/{index}")

	public ResponseEntity<?> enrollStudent(@PathVariable String index, @RequestBody Student student,
			HttpSession httpSession)
	// public ResponseEntity<?> enrollStudent(@PathVariable String
	// index,@RequestBody StringForJson stringForJson,HttpSession httpSession)
	{

		elasticService.addStudent(index, student, httpSession);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", httpSession.getAttribute("response"));
		map.put("status", true);
		map.put("code", 200);
		map.put("message", "successfully enrolled student");
		return ResponseEntity.ok(map);
	}
//		ObjectMapper mapper = new ObjectMapper();
//		  String jsonString = "{\"name\":\"sadab\",\"gender\": \"male\",\"course\": \"elasticsearch\"}";
//	    try{
//	       StringForJson user = mapper.readValue(jsonString, StringForJson.class);
//	      
//	      jsonString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(user);
//	      
//	       System.err.println(jsonString);
//	    }
//	    catch (JsonParseException e) { 
//	    	e.printStackTrace();
//	    	}
//	    catch (JsonMappingException e) {
//	    	e.printStackTrace();
//	    	}
//	    catch (IOException e) {
//	    	e.printStackTrace();
//	    	}
//		Map<String, Object> map = new HashMap<String, Object>();
//		map.put("response", httpSession.getAttribute("response"));
//		map.put("status", true);
//		map.put("code", 200);
//		map.put("message", "successfully enrolled student");
//		return ResponseEntity.ok(map);
//	}
//	

	@GetMapping("/view-student/{index}")
	public ResponseEntity<?> getStudent(@PathVariable String index, HttpSession httpSession) throws IOException {
		elasticService.getStudent(index, httpSession);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", httpSession.getAttribute("res"));
		map.put("status", true);
		map.put("code", 200);
		map.put("message", "successfully got students list");
		return ResponseEntity.ok(map);
	}

	@GetMapping("/view-student-by-id/{index}/{id}")
	public ResponseEntity<?> getStudentById(@PathVariable String id, @PathVariable String index,
			HttpSession httpSession) {
		elasticService.getStudentById(index, id, httpSession);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", httpSession.getAttribute("studentWithId"));
		map.put("status", true);
		map.put("code", 200);
		map.put("message", "successfully got student");
		elasticService.matchSearch(httpSession);
		map.put("boolQuery", httpSession.getAttribute("boolQueryResult"));
		return ResponseEntity.ok(map);
	}

	@GetMapping("/view-student")
	public ResponseEntity<?> viewStudent(@RequestParam String firstMatchingField,
			@RequestParam String secondMatchingField, @RequestParam String firstMatchingValue,
			@RequestParam String secondMatchingValue, HttpSession httpSession) {
		elasticService.getMatchingStudent(firstMatchingField, firstMatchingValue, secondMatchingField,
				secondMatchingValue, httpSession);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", httpSession.getAttribute("matchingStudents"));
		map.put("status", true);
		map.put("code", 200);
		map.put("message", "Fetched matchng record");
		return ResponseEntity.ok(map);
	}

	/*
	 * Check at runtime, one @PathVariable...
	 */
	@DeleteMapping("/deleteIndex/{index}/{id}")
	public ResponseEntity<?> deleteIndex(@PathVariable String index, @PathVariable String id) {
		elasticService.deleteStudentById(index, id);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("status", true);
		map.put("code", 200);
		map.put("message", "deleted successfully with index : " + index + " and id is : " + id);
		return ResponseEntity.ok(map);
	}

	@DeleteMapping("/deleteIndexValue/{index}")
	public ResponseEntity<?> deleteIndexValue(@PathVariable String index) {
		elasticService.deleteIndex(index);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("status", true);
		map.put("code", 200);
		map.put("message", "deleted successfully with index : " + index);
		return ResponseEntity.ok(map);
	}

	@PutMapping("/updateIndexValue/{index}/{id}")
	public ResponseEntity<?> updateIndexValue(@RequestBody Student student, @PathVariable String index,
			@PathVariable String id, HttpSession httpSession) throws IOException {
		elasticService.updateStudentDetails(index, id, student, httpSession);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", httpSession.getAttribute("updateRes"));
		map.put("status", true);
		map.put("code", 200);
		map.put("message", "updated successfully with index : " + index);
		return ResponseEntity.ok(map);
	}

	@GetMapping("/aggregations/{index}")
	public ResponseEntity<?> aggregationFun(@RequestParam String caseValue, @PathVariable String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", elasticService.aggregationFunctions(caseValue, index).getBody());
		map.put("code", elasticService.aggregationFunctions(caseValue, index).getStatusCodeValue());
		map.put("status", elasticService.aggregationFunctions(caseValue, index).getStatusCode());
		map.put("message", "Got aggregation record successfully");
		return ResponseEntity.ok(map);
	}

	@GetMapping("/cardinality/{index}")
	public ResponseEntity<?> cardinality() {
		Map<String, Object> map = new HashMap<String, Object>();
		elasticService.cardinalityFunc();
		map.put("response", elasticService.cardinalityFunc().getBody());
		map.put("code", elasticService.cardinalityFunc().getStatusCodeValue());
		map.put("status", elasticService.cardinalityFunc().getStatusCode());
		map.put("message", "Got cardinality record successfully");
		return ResponseEntity.ok(map);
	}

	@GetMapping("/percentile/{index}")
	public ResponseEntity<?> Percentile(@PathVariable String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", elasticService.percentile(index).getBody());
		map.put("code", elasticService.percentile(index).getStatusCode());
		map.put("status", elasticService.percentile(index).getStatusCode());
		map.put("messeage", "Got percentile successfully");
		return ResponseEntity.ok(map);

	}

	@GetMapping("/top-hits/{index}")
	public ResponseEntity<?> topHitsFunc(@PathVariable String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", elasticService.topHits(index).getBody());
		map.put("code", elasticService.topHits(index).getStatusCode());
		map.put("status", elasticService.topHits(index).getStatusCode());
		map.put("messeage", "Got hits successfully");
		return ResponseEntity.ok(map);
	}

	@GetMapping("/filters/{index}")
	public ResponseEntity<?> filterFunc(@PathVariable String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", elasticService.filterFunc(index).getBody());
		map.put("code", elasticService.filterFunc(index).getStatusCode());
		map.put("status", elasticService.filterFunc(index).getStatusCode());
		map.put("messeage", "Got filters data successfully");
		return ResponseEntity.ok(map);
	}

	@GetMapping("/date/{index}")
	public ResponseEntity<?> DateAgg(@PathVariable String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", elasticService.DateAgg(index));
		map.put("code", elasticService.DateAgg(index).getStatusCode());
		map.put("status", elasticService.DateAgg(index).getStatusCode());
		map.put("message", "Got date successfully");

		return ResponseEntity.ok(map);
	}

	@GetMapping("/ip/{index}")
	public ResponseEntity<?> Iprange(@PathVariable String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", elasticService.Iprange(index).getBody());
		map.put("code", elasticService.Iprange(index).getStatusCode());
		map.put("status", elasticService.Iprange(index).getStatusCode());
		map.put("message", "Got Ip range successfully");

		return ResponseEntity.ok(map);
	}

	@GetMapping("/histogram/{index}")
	public ResponseEntity<?> histogram(@PathVariable String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", elasticService.histogram(index).getBody());
		map.put("code", elasticService.histogram(index).getStatusCode());
		map.put("status", elasticService.histogram(index).getStatusCode());
		map.put("message", "Got histogram range successfully");

		return ResponseEntity.ok(map);
	}

	@GetMapping("/datehistogram/{index}")
	public ResponseEntity<?> datehistogram(@PathVariable String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", elasticService.datehistogram(index).getBody());
		map.put("code", elasticService.datehistogram(index).getStatusCode());
		map.put("status", elasticService.datehistogram(index).getStatusCode());
		map.put("message", "Got histogram date successfully");

		return ResponseEntity.ok(map);
	}

	@GetMapping("/geo/{index}")
	public ResponseEntity<?> geodistance(@PathVariable String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", elasticService.geodistance(index).getBody());
		map.put("code", elasticService.geodistance(index).getStatusCode());
		map.put("status", elasticService.geodistance(index).getStatusCode());
		map.put("message", "Got geo distance successfully");
		return ResponseEntity.ok(map);
	}

	@GetMapping("/geogrid/{index}")
	public ResponseEntity<?> geogrid(@PathVariable String index) throws IOException {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", elasticService.geogrid(index).getBody());
		map.put("code", elasticService.geogrid(index).getStatusCode());
		map.put("status", elasticService.geogrid(index).getStatusCode());
		map.put("message", "Got geo grid successfully");
		return ResponseEntity.ok(map);
	}

	@PutMapping("/createMapping")
	public ResponseEntity<?> createMapping(@RequestBody String text) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("", elasticService.DemoinCreateMappingStatic(text));
		return ResponseEntity.ok(map);
	}

	@GetMapping("/fieldmapping")
	public ResponseEntity<?> CreateFiledMapping(@RequestBody String text) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("", elasticService.DemoinCreateFiledMapping(text));
		return ResponseEntity.ok(map);
	}

	@PutMapping("/template")
	public ResponseEntity<?> CreatetempalteStatic(@RequestBody String text) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("", elasticService.DemoinCreatetempalteStatic(text));
		return ResponseEntity.ok(map);
	}

	@GetMapping("/scroll")
	public ResponseEntity<?> scrollDemo() throws IOException {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("", elasticService.scrollImplement().getBody());
		return ResponseEntity.ok(map);
	}

	@GetMapping("/scrolls")
	public ResponseEntity<?> scrollsDemo() throws IOException {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", elasticService.scrollsImplement().getBody());
		return ResponseEntity.ok(map);
	}

	@GetMapping("/Analyzer")
	public ResponseEntity<?> Analyzer() throws IOException {

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", elasticService.Analyzer());
		return ResponseEntity.ok(map);

	}

	@GetMapping("/nested/{index}")
	public ResponseEntity<?> nested(@PathVariable String index) throws IOException {

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", elasticService.NestedAggregation(index).getBody());
		return ResponseEntity.ok(map);

	}

	@GetMapping("/global")
	public ResponseEntity<?> globalAgg() throws IOException {

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", elasticService.globalAggregation().getBody());
		return ResponseEntity.ok(map);
	}

	@GetMapping("/getPage/{from}/{id}")
    public ResponseEntity<?> getAllByPaggination(@PathVariable Integer from,@PathVariable String id) throws IOException {
         return elasticService.getAllByPaggination(from, id);

    }
	

}