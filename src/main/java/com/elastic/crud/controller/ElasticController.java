package com.elastic.crud.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpSession;

import org.elasticsearch.action.search.SearchResponse;
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

import com.elastic.crud.model.Student;
import com.elastic.crud.service.ElasticConnectService;

@Controller
@RequestMapping("/student")
public class ElasticController
{
	
	@Autowired
	ElasticConnectService elasticService;
	
	@PostMapping("/enroll/{index}")
	public ResponseEntity<?> enrollStudent(@PathVariable String index,@RequestBody Student student,HttpSession httpSession)
	{
		elasticService.addStudent(index,student,httpSession);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", httpSession.getAttribute("response"));
		map.put("status", true);
		map.put("code", 200);
		map.put("message", "successfully enrolled student");
		return ResponseEntity.ok(map);
	}
	
	@GetMapping("/view-student/{index}")
	public ResponseEntity<?> getStudent(@PathVariable String index,HttpSession httpSession) throws IOException
	{
		elasticService.getStudent(index,httpSession);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", httpSession.getAttribute("res"));
		map.put("status", true);
		map.put("code", 200);
		map.put("message", "successfully got students list");
		return ResponseEntity.ok(map);
	}

	
	@GetMapping("/view-student-by-id/{index}/{id}")
	public ResponseEntity<?> getStudentById(@PathVariable String id,@PathVariable String index,HttpSession httpSession)
	{
		elasticService.getStudentById(index,id,httpSession);
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
	public ResponseEntity<?> viewStudent(@RequestParam String firstMatchingField,@RequestParam String secondMatchingField,@RequestParam String firstMatchingValue,@RequestParam String secondMatchingValue,HttpSession httpSession){
		elasticService.getMatchingStudent(firstMatchingField,firstMatchingValue,secondMatchingField,secondMatchingValue,httpSession);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", httpSession.getAttribute("matchingStudents"));
		map.put("status", true);
		map.put("code", 200);
		map.put("message", "Fetched matchng record");
		return ResponseEntity.ok(map);
	}
	/*
	 * Check at runtime, one @PathVariable...
	 * */
	@DeleteMapping("/deleteIndex/{index}/{id}")
	public ResponseEntity<?> deleteIndex(@PathVariable String index,@PathVariable String id)
	{
		elasticService.deleteStudentById(index,id);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("status", true);
		map.put("code", 200);
		map.put("message", "deleted successfully with index : " + index + " and id is : " + id);
		return ResponseEntity.ok(map);
	}
	
	@DeleteMapping("/deleteIndexValue/{index}")
	public ResponseEntity<?> deleteIndexValue(@PathVariable String index)
	{
		elasticService.deleteIndex(index);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("status", true);
		map.put("code", 200);
		map.put("message", "deleted successfully with index : " + index);
		return ResponseEntity.ok(map);
	}
	
	@PutMapping("/updateIndexValue/{index}/{id}")
	public ResponseEntity<?> updateIndexValue(@RequestBody Student student,@PathVariable String index,@PathVariable String id,HttpSession httpSession) throws IOException
	{
		elasticService.updateStudentDetails(index, id, student,httpSession);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("response", httpSession.getAttribute("updateRes"));
		map.put("status", true);
		map.put("code", 200);
		map.put("message", "updated successfully with index : " + index);
		return ResponseEntity.ok(map);
	}
}