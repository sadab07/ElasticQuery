package com.elastic.crud.serviceImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGrid.*;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;

import javax.servlet.http.HttpSession;
//import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoGrid;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedHistogram;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNested;
import org.elasticsearch.search.aggregations.bucket.range.ParsedBinaryRange;
import org.elasticsearch.search.aggregations.bucket.range.ParsedDateRange;
import org.elasticsearch.search.aggregations.bucket.range.ParsedGeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.Range.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.client.indices.AnalyzeResponse.AnalyzeToken;
import org.elasticsearch.client.indices.DetailAnalyzeResponse;
import org.elasticsearch.client.indices.DetailAnalyzeResponse.AnalyzeTokenList;
import org.elasticsearch.client.indices.GetFieldMappingsRequest;
import org.elasticsearch.client.indices.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import com.elastic.crud.config.ElasticConfig;
import com.elastic.crud.model.Student;
import com.elastic.crud.service.ElasticConnectService;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.index.query.TermQueryBuilder;
import java.util.LinkedList;
import org.json.JSONObject;

@Service
public class ElasticConnectServiceImpl implements ElasticConnectService {

	@Autowired
	private ElasticConnectService elasticService;
	Map<String, Object> mapIp;
	RestHighLevelClient client = ElasticConfig.elasticSearchConn();

	@Override
	public Map<String, Object> addStudent(String index, Student student, HttpSession httpSession) {
		/*
		 * For creating index and inserting a record
		 */

		IndexRequest request = new IndexRequest("sadab2");
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("name", student.getName());
		map.put("age", student.getAge());
		map.put("date", student.getDate());
		map.put("ip", student.getIp());
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
	public SearchResponse getStudent(String index, HttpSession httpSession) throws IOException {

		Map<String, Object> map = new HashMap<>();
		/*
		 * For searching all details of index
		 */
		SearchRequest searchRequestForAll = new SearchRequest(index);

		MatchAllQueryBuilder matchQueryBuilderForAll = QueryBuilders.matchAllQuery();
		SearchSourceBuilder sourceBuilderForAll = new SearchSourceBuilder();
		sourceBuilderForAll.query(matchQueryBuilderForAll);
		searchRequestForAll.source(sourceBuilderForAll);

		SearchResponse searchResponseForAll = client.search(searchRequestForAll, RequestOptions.DEFAULT);

		List list = new ArrayList();

		for (SearchHit s : searchResponseForAll.getHits().getHits()) {
			list.add(s.getSourceAsMap());
		}

		httpSession.setAttribute("res", list);
		map.put("response", list);
		map.put("code", 200);
		map.put("message", "Got response from elastic search's database successfully...");
		map.put("status", true);
		return searchResponseForAll;
	}

	@Override
	public Student getStudentById(String index, String id, HttpSession httpSession) {
		SearchRequest searchRequest = new SearchRequest(index);

		MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("_id", id);
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(matchQueryBuilder);
		searchRequest.source(sourceBuilder);
		List list = new ArrayList();
		try {
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
			for (SearchHit s : searchResponse.getHits().getHits()) {
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
	public void getMatchingStudent(String firstMatchingField, String firstMatchingValue, String secondMatchingField,
			String secondMatchingValue, HttpSession httpSession) {

		MultiSearchRequest request = new MultiSearchRequest();

		SearchRequest firstSearchRequest = new SearchRequest("jay");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		// searchSourceBuilder.query(QueryBuilders.matchQuery(firstMatchingField,
		// firstMatchingValue));
		searchSourceBuilder.query(QueryBuilders.matchQuery("name.keyword", "JS"));
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
	public boolean deleteStudentById(String index, String id) {
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
	public UpdateResponse updateStudentDetails(String index, String id, Student student, HttpSession httpSession)
			throws IOException {
		/*
		 * For updating index value commented because value will update when we run the
		 * program everytime...
		 */
		UpdateRequest updateRequest = new UpdateRequest(index, id);
		ObjectMapper om = new ObjectMapper();
		String jsonString = om.writeValueAsString(student);
		updateRequest.doc(jsonString, XContentType.JSON);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("name", student.getName());
		map.put("age", student.getAge());
		map.put("date", student.getDate());
		map.put("ip", student.getIp());
//	    	map.put("gender", student.getGender());
//	    	map.put("subject", student.getSubject());
		httpSession.setAttribute("updateRes", map);
		UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
		return updateResponse;
	}

//........................ Aggregation..................................................  //
	@Override
	public ResponseEntity<?> aggregationFunctions(String caseValue, String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		if (caseValue.equalsIgnoreCase("sum") || caseValue.equalsIgnoreCase("avg") || caseValue.equalsIgnoreCase("min")
				|| caseValue.equalsIgnoreCase("max")) {
			try {
				switch (caseValue.toLowerCase()) {
				case "sum":
					SearchRequest requestSum = new SearchRequest(index);
					SearchSourceBuilder builderSum = new SearchSourceBuilder();
					SumAggregationBuilder aggregationSum = AggregationBuilders.sum("aggSum").field("Age");
					builderSum.aggregation(aggregationSum);
					requestSum.source(builderSum);
					SearchResponse searchResponseSum = client.search(requestSum, RequestOptions.DEFAULT);
					Aggregations aggregationsSum = searchResponseSum.getAggregations();
					ParsedSum psum = aggregationsSum.get("aggSum");
					map.put("sum", psum.getValue());
					break;

				case "avg":
					SearchRequest requestAvg = new SearchRequest(index);
					SearchSourceBuilder builderAvg = new SearchSourceBuilder();
					AvgAggregationBuilder aggregationAvg = AggregationBuilders.avg("aggAvg").field("age");
					builderAvg.aggregation(aggregationAvg);
					requestAvg.source(builderAvg);
					SearchResponse searchResponseAvg = client.search(requestAvg, RequestOptions.DEFAULT);
					Aggregations aggregationsAvg = searchResponseAvg.getAggregations();
					ParsedAvg pavg = aggregationsAvg.get("aggAvg");
					map.put("avg", pavg.getValue());
					break;

				case "min":
					SearchRequest request = new SearchRequest(index);
					SearchSourceBuilder builder = new SearchSourceBuilder();
					MinAggregationBuilder min = AggregationBuilders.min("aggForMin").field("Age");
					builder.aggregation(min);
					request.source(builder);
					SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
					Aggregations aggregation = searchResponse.getAggregations();
					ParsedMin pmin = aggregation.get("aggForMin");
					map.put("min", pmin.getValue());

					break;

				case "max":
					MaxAggregationBuilder max = AggregationBuilders.max("aggForMax").field("Age");
					SearchSourceBuilder builderMax = new SearchSourceBuilder();
					SearchRequest requestMax = new SearchRequest("sa");
					builderMax.aggregation(max);
					requestMax.source(builderMax);
					SearchResponse searchResponseMax = client.search(requestMax, RequestOptions.DEFAULT);
					Aggregations aggregationMax = searchResponseMax.getAggregations();
					ParsedMax pmax = aggregationMax.get("aggForMax");
					map.put("max", pmax.getValue());
					break;

				default:
					System.err.println("Wrong keyword");
					break;
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			map.put("error", "Please enter keyword from 'sum','avg','min','max'");
		}
		return ResponseEntity.ok(map);
	}

	// ........................
	// cardinalityFunc.................................................. //

	public ResponseEntity<?> cardinalityFunc() {
		Map<String, Object> map = new HashMap<String, Object>();
		SearchSourceBuilder builderCar = new SearchSourceBuilder();
		CardinalityAggregationBuilder aggregation = AggregationBuilders.cardinality("aggCar").field("Age");

		SearchRequest requestCar = new SearchRequest("sa");
		requestCar.source(builderCar);

		builderCar.aggregation(aggregation);
		requestCar.source(builderCar);
		try {
			SearchResponse searchResponseCar = client.search(requestCar, RequestOptions.DEFAULT);
			Aggregations agg = searchResponseCar.getAggregations();
			Cardinality pc = (Cardinality) agg.get("aggCar");
			map.put("cardinality", pc.getValue());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return ResponseEntity.ok(map);
	}

	// ........................
	// percentile.................................................. //

	public ResponseEntity<?> percentile(String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		SearchRequest requestPercent = new SearchRequest(index);
		SearchSourceBuilder builderPercent = new SearchSourceBuilder();
		PercentilesAggregationBuilder aggregationPercent = AggregationBuilders.percentiles("aggPercent").field("Age");
		// .field("Age").percentiles(1.0, 5.0, 10.0, 20.0, 30.0, 75.0, 95.0, 99.0);
		builderPercent.aggregation(aggregationPercent);
		requestPercent.source(builderPercent);
		SearchResponse searchResponsePercent;
		try {
			searchResponsePercent = client.search(requestPercent, RequestOptions.DEFAULT);
			Aggregations aggregationsPercent = searchResponsePercent.getAggregations();
			Aggregations aggPercent = searchResponsePercent.getAggregations();
			Percentiles p = aggPercent.get("aggPercent");
			List listValue = new ArrayList();
			List listPercentage = new ArrayList();
			for (Percentile per : p) {
				listValue.add(per.getValue());
				listPercentage.add(per.getPercent());
			}
			map.put("value", listValue);
			map.put("percentage", listPercentage);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ResponseEntity.ok(map);
	}
	// TODO Auto-generated method stub

	// ........................
	// topHits.................................................. //

	@Override
	public ResponseEntity<?> topHits(String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		SearchRequest requestHits = new SearchRequest(index);
		SearchSourceBuilder builderHits = new SearchSourceBuilder();
		AggregationBuilder aggregationHits = AggregationBuilders.terms("aggHits").field("Age")
				.subAggregation(AggregationBuilders.topHits("top"));
		builderHits.aggregation(aggregationHits);
		requestHits.source(builderHits);
		SearchResponse searchResponsePercent;
		try {
			searchResponsePercent = client.search(requestHits, RequestOptions.DEFAULT);
			Aggregations aggregationsPercent = searchResponsePercent.getAggregations();
			Aggregations aggPercent = searchResponsePercent.getAggregations();
//							    Terms aggHits = searchResponsePercent.getAggregations().get("aggHits");
			Terms terms = aggPercent.get("aggHits");
			List listKey = new ArrayList();
			List docs = new ArrayList();
			for (Terms.Bucket entry : terms.getBuckets()) {
				listKey.add(entry.getKey());
				docs.add(entry.getDocCount());

				// We ask for top_hits for each bucket
				TopHits topHits = entry.getAggregations().get("top");
				List listHits = new ArrayList();
				List listData = new ArrayList();
				for (SearchHit hit : topHits.getHits().getHits()) {
					listHits.add(hit.getId());
					listData.add(hit.getSourceAsString());
				}
				map.put("hits_id", listHits);
				map.put("hits_data", listData);
				map.put("bucket_key", listKey);
				map.put("doc_count", docs);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ResponseEntity.ok(map);

	}

	// ...................... filterFunc.................................................. //

	public ResponseEntity<?> filterFunc(String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		SearchRequest requestPercent = new SearchRequest(index);
		SearchSourceBuilder builderPercent = new SearchSourceBuilder();
		FiltersAggregationBuilder filter = AggregationBuilders.filters("agg",
				new FiltersAggregator.KeyedFilter("Age", QueryBuilders.termQuery("Age", 22)));
		builderPercent.aggregation(filter);
		requestPercent.source(builderPercent);

		try {
			SearchResponse searchResponse = client.search(requestPercent, RequestOptions.DEFAULT);

			Filters pf = searchResponse.getAggregations().get("agg");
			// For each entry
			for (Filters.Bucket entry : pf.getBuckets()) {
				String key = entry.getKeyAsString(); // bucket key
				long docCount = entry.getDocCount(); // Doc count
				map.put("filter_key", key);
				map.put("doc_count", docCount);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ResponseEntity.ok(map);
	}
	// .......................................date Range..........................//

	public ResponseEntity<?> DateAgg(String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		SearchRequest requestdate = new SearchRequest(index);
		SearchSourceBuilder builderdate = new SearchSourceBuilder();
		AggregationBuilder aggregation = AggregationBuilders.dateRange("agg").field("bdate")
				.format("yyyy-MM-dd||dd-MM-yyyy")
				// .addUnboundedTo("2026") // from -infinity to 1950 (excluded)
				.addRange("1995-12-07", "04-07-2000"); // from 1950 to 1960 (excluded)
		// .addUnboundedFrom("2021");
		builderdate.aggregation(aggregation);
		requestdate.source(builderdate);
		List<Object> list = new ArrayList();
		try {
			SearchResponse searchResponse = client.search(requestdate, RequestOptions.DEFAULT);
			Aggregations date = searchResponse.getAggregations();
			ParsedDateRange pvd = date.get("agg");
			for (Bucket entry : pvd.getBuckets()) {
				String key = entry.getKeyAsString(); // bucket key
				long docCount = entry.getDocCount(); // Doc count
				// map.put("filter_key", key);
				// map.put("doc_count", docCount);
				list.add("Key : " + key + " Count : " + docCount);
			}
			map.put("list", list);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ResponseEntity.ok(map);
	}

	// ....................Ip range............................//

	public ResponseEntity<?> Iprange(String index) {

		SearchRequest requestip = new SearchRequest(index);
		SearchSourceBuilder builderip = new SearchSourceBuilder();
		AggregationBuilder aggregation = AggregationBuilders.ipRange("ipagg").field("ip")
//		                .addUnboundedTo("10.1.1.168")             // from -infinity to 192.168.1.0 (excluded)
				// .addRange("10.1.1.160", "10.3.3.168"); // from 192.168.1.0 to 192.168.2.0
				// (excluded)
				.addRange("10.1.1.10", "10.7.7.70");
//	                .addUnboundedFrom("10.3.3.160");

		builderip.aggregation(aggregation);
		requestip.source(builderip);

		try {
			SearchResponse response = client.search(requestip, RequestOptions.DEFAULT);
			Aggregations IP = response.getAggregations();
			ParsedBinaryRange pbr = IP.get("ipagg");

//			List<?> count=IP.asList();
//			mapIp.put("list", count);
			// For each entry
			for (org.elasticsearch.search.aggregations.bucket.range.Range.Bucket entry : pbr.getBuckets()) {
				mapIp = new HashMap<String, Object>();
				String key = entry.getKeyAsString(); // Ip range as key
				String fromAsString = entry.getFromAsString(); // Ip bucket from as a String
				String toAsString = entry.getToAsString(); // Ip bucket to as a String
				long docCount = entry.getDocCount();
				mapIp.put("key", key);
				mapIp.put("fromAsString", fromAsString);
				mapIp.put("toAsString", toAsString);
				mapIp.put("docCount", docCount);

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block

			e.printStackTrace();
		}

		return ResponseEntity.ok(mapIp);
	}

	// .....................histogram..........................................//

	public ResponseEntity<?> histogram(String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		SearchRequest requesthistogram = new SearchRequest(index);
		SearchSourceBuilder builderhistogram = new SearchSourceBuilder();
		AggregationBuilder aggregation = AggregationBuilders.histogram("histoagg").field("age").interval(1);

		builderhistogram.aggregation(aggregation);
		requesthistogram.source(builderhistogram);

		try {
			SearchResponse response = client.search(requesthistogram, RequestOptions.DEFAULT);
			Aggregations histogram = response.getAggregations();
			ParsedHistogram pbr = histogram.get("histoagg");    
		
			List<Object> list = new ArrayList();
			// For each entry
			for (org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket entry : pbr.getBuckets()) {
				Number key = (Number) entry.getKey(); // Key
				long docCount = entry.getDocCount(); // Doc count
				list.add("Key : " + key + " Count : " + docCount);

				// map.put("key", key);
				// map.put("docCount", docCount);
				System.err.println(key + " " + docCount);
			}
			map.put("list", list);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return ResponseEntity.ok(map);
	}

	// ..................... date
	// histogram..........................................//

	public ResponseEntity<?> datehistogram(String index) {
		Map<String, Object> map = new HashMap<String, Object>();
		SearchRequest requestdatehistogram = new SearchRequest(index);
		SearchSourceBuilder builderdatehistogram = new SearchSourceBuilder();
		AggregationBuilder aggregation = AggregationBuilders.dateHistogram("datehistoagg").field("date")
				.calendarInterval(DateHistogramInterval.YEAR);
		builderdatehistogram.aggregation(aggregation);
		requestdatehistogram.source(builderdatehistogram);
		try {
			SearchResponse response = client.search(requestdatehistogram, RequestOptions.DEFAULT);
			Aggregations datehistogram = response.getAggregations();
			ParsedDateHistogram pbr = datehistogram.get("datehistoagg");

//				for (Bucket entry : pbr.getBuckets()) {
//				    Number key = (Number) entry.getKey();   // Key
//				    long docCount = entry.getDocCount();    // Doc count
//				    map.put("key", key);
//				    map.put("docCount", docCount);
//				    System.err.println(key + " " + docCount);
//				}
			// Filters agg = datehistogram.get("datehistoagg");
			List<Object> list = new ArrayList();
			for (org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket entry : pbr.getBuckets()) {
				String key = entry.getKeyAsString(); // bucket key
				long docCount = entry.getDocCount(); // Doc count
				// map.put("filter_key", key);
				// map.put("doc_count", docCount);
				list.add("Key : " + key + " Count : " + docCount);

			}
			map.put("list", list);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return ResponseEntity.ok(map);
	}

	// ............field mapping..........................//

	public Map<String, Object> DemoinCreateMappingStatic(String json) {
		Map<String, Object> map = new HashMap<String, Object>();
		JSONObject jsonObj = new JSONObject(json);

		System.err.println(jsonObj);
		PutMappingRequest request = new PutMappingRequest("sadab2");
		request.source("{\n" + "  \"properties\": {\n" + "    \"name\": {\n" + "      \"type\": \"text\"\n" + "    },\n"
				+ "    \"age\": {\n" + "      \"type\": \"integer\"\n" + "    },\n" + "    \"ip\": {\n"
				+ "      \"type\": \"ip\"\n" + "    },\n" + "    \"date\": {\n" + "      \"type\": \"date\"\n"
				+ "    }\n" + "  }\n" + "}", XContentType.JSON);

		try {
			AcknowledgedResponse putMappingResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);
			map.put("PutMappingResaponse", putMappingResponse);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return map;
	}

	// -------------!!!!!!!!!!!! Create field mapping
	// ---------------------------------------!!!!!!!//
	@Override
	public Map<String, Object> DemoinCreateFiledMapping(String text) {
		JSONObject jsonObj = new JSONObject();
		ObjectMapper om = new ObjectMapper();
		Map<String, Object> map = new HashMap<String, Object>();
		try {

			GetFieldMappingsRequest request = new GetFieldMappingsRequest();
			request.indices("sadb1");
			// request.fields("age");
			request.fields("name", "age", "ip", "date");
			request.indicesOptions(IndicesOptions.lenientExpandOpen());
			GetFieldMappingsResponse response = client.indices().getFieldMapping(request, RequestOptions.DEFAULT);

			om.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
			map.put("FiledMappinResponse", response.toString());
			System.err.println(response);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return map;
	}

	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<< create template<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<//
	@Override
	public Map<String, Object> DemoinCreatetempalteStatic(String text) {
		JSONObject jsonObj = new JSONObject();
		Map<String, Object> map = new HashMap<String, Object>();
		PutIndexTemplateRequest request = new PutIndexTemplateRequest("mytemplate1");

		List<String> indexPatterns = null;
		indexPatterns.add("aaa");
		request.patterns(indexPatterns);
		request.settings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 1));
		request.mapping("{\n" + "  \"properties\": {\n" + "    \"message\": {\n" + "      \"type\": \"text\"\n"
				+ "    }\n" + "  }\n" + "}", XContentType.JSON);
		request.create(true);
		map.put("Ans", request);
//			Map<String, Object> jsonMap = new HashMap<>();
//			{
//			    Map<String, Object> properties = new HashMap<>();
//			    {
//			        Map<String, Object> message = new HashMap<>();
//			        message.put("type", "text");
//			        properties.put("message", message);
//			    }
//			    jsonMap.put("properties", properties);
//			}
//			REQUEST.MAPPING(JSONMAP);
		return map;
	}

	// .........................geo distance ........................//
	public ResponseEntity<?> geodistance(String index) {
		Map<String, Object> map = new HashMap<>();
		SearchRequest requestgeo = new SearchRequest(index);
		SearchSourceBuilder buildergeo = new SearchSourceBuilder();
		AggregationBuilder aggregation = AggregationBuilders
				.geoDistance("geoagg", new GeoPoint(48.84237171118314, 2.33320027692004)).field("lat.location")
				.unit(DistanceUnit.KILOMETERS).addUnboundedTo(3.0).addRange(50.0, 130.0);
		buildergeo.aggregation(aggregation);
		requestgeo.source(buildergeo);

		try {
			SearchResponse response = client.search(requestgeo, RequestOptions.DEFAULT);
			Aggregations geo = response.getAggregations();
			ParsedGeoDistance pbr = geo.get("geoagg");
			List<Object> list = new ArrayList();
			for (Bucket entry : pbr.getBuckets()) {
				String key = entry.getKeyAsString(); // key as String
				Number from = (Number) entry.getFrom(); // bucket from value
				Number to = (Number) entry.getTo(); // bucket to value
				long docCount = entry.getDocCount();
				list.add("Key : " + key + " Count : " + docCount + " From : " + from + " To : " + to);
				map.put("docCount", docCount);
			}
			map.put("list", list);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return ResponseEntity.ok(map);
	}

	
	//............................... GEOGRID..........................................// 
	public ResponseEntity<?> geogrid(String index) throws IOException {
		Map<String, Object> map = new HashMap<>();
		SearchRequest requestgeogrid = new SearchRequest(index);
		SearchSourceBuilder buildergeogrid = new SearchSourceBuilder();
	AggregationBuilder aggregation =
	        AggregationBuilders
	                .geohashGrid("geoagg")
	                .field("location.location")
	                .precision(4);
	
	buildergeogrid.aggregation(aggregation);
	requestgeogrid.source(buildergeogrid);
	
	SearchResponse response = client.search(requestgeogrid, RequestOptions.DEFAULT);
	Aggregations geo = response.getAggregations();
	ParsedGeoGrid pgg = geo.get("geoagg");
	System.err.println("pgg.getBuckets() : " + pgg.getBuckets());
	List<Object> list = new ArrayList();
//	for (org.elasticsearch.search.aggregations.bucket.geogrid.GeoGrid.Bucket entry : pbr.getBuckets()) {
//		String key = entry.getKeyAsString(); // key as String
//		System.err.println("Key " + key);
//		 GeoPoint key1 = (GeoPoint) entry.getKey();
//		 System.err.println("Key 1 " + key1);
//		Number from = (Number) ((Bucket) entry).getFrom(); // bucket from value
//		System.err.println("From " + from);
//		Number to = (Number) entry.getKey(); // bucket to value
//		System.err.println("To " + to);
//		long docCount = entry.getDocCount();
//		list.add("Key : " + key + " Count : " + docCount + " From : " + from + " To : " + to);
//		map.put("docCount", docCount);
//	}
//	map.put("list", list);
	GeoHashGridAggregationBuilder agg = response.getAggregations().get("geoagg");
	
	for (GeoGrid.Bucket entry : ((GeoGrid) agg).getBuckets()) {
	    String keyAsString = entry.getKeyAsString(); // key as String
	    GeoPoint key = (GeoPoint) entry.getKey();    // key as geo point
	    long docCount = entry.getDocCount();
	    System.err.println(docCount + " " + keyAsString + " " + key);
	}
	return ResponseEntity.ok(map);
	}

	
	
	
	// ...........................Scroll..............................//

	@Override
	public ResponseEntity<?> scrollImplement() throws IOException {
		Map<String, Object> map = new LinkedHashMap<String, Object>();
		SearchRequest searchRequest = new SearchRequest("kibana_sample_data_flights");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.size(5);
		searchRequest.source(searchSourceBuilder);
		searchRequest.scroll(TimeValue.timeValueMinutes(1L));
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		String scrollId = searchResponse.getScrollId();
		SearchHits hits = searchResponse.getHits();
		map.put("scroll_id", scrollId);
		map.put("hits", hits);
		return ResponseEntity.ok(map);
	}

	// .....................Scrolls......................................//

	@Override
	public ResponseEntity<?> scrollsImplement() throws IOException {
		Map<String, Object> map = new LinkedHashMap<String, Object>();
		List<Object> list = new LinkedList<Object>();
		SearchRequest searchRequest = new SearchRequest("kibana_sample_data_flights").scroll(new TimeValue(10000));
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		TermQueryBuilder qb = QueryBuilders.termQuery("OriginCityName", "Naples");
		searchSourceBuilder.query((org.elasticsearch.index.query.QueryBuilder) qb);
		searchSourceBuilder.sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC);
		searchRequest.source(searchSourceBuilder);
		SearchResponse scrollResp = client.search(searchRequest, RequestOptions.DEFAULT);

		for (SearchHit s : scrollResp.getHits().getHits()) {
			list.add(s);
		}
		map.put("response", list);
		return ResponseEntity.ok(map);
	}

	// ....................Analyzer..........................................//

	public List<AnalyzeToken> Analyzer() throws IOException {

		AnalyzeRequest request = AnalyzeRequest.withField("kibana_sample_data_flights", "OriginCountry", "US");

		// AnalyzeRequest request = AnalyzeRequest.withGlobalAnalyzer(analyzer, "US");
		// AnalyzeRequest request =
		// AnalyzeRequest.withIndexAnalyzer("kibana_sample_data_flights", analyzer,
		// "Zurich");
		// AnalyzeRequest request =
		// AnalyzeRequest.withNormalizer("kibana_sample_data_flights", normalizer,
		// "Zurich");

//			request.attributes("keyword", "type");
		request.analyzer();
		AnalyzeResponse response = client.indices().analyze(request, RequestOptions.DEFAULT);
//			DetailAnalyzeResponse detail = response.detail();
//			System.err.println(detail);
		return response.getTokens();
	}
	// ........................Nested Aggregation...........................................//

	public ResponseEntity<?> NestedAggregation(String index) {
		Map<String, Object> map = new HashMap<>();
		SearchRequest requestNested = new SearchRequest(index);
		SearchSourceBuilder builderNested = new SearchSourceBuilder();
		AggregationBuilder aggregation = AggregationBuilders.nested("agg", "name")
		.subAggregation(AggregationBuilders.ipRange("agg").field("ip").addRange("10.1.1.10", "10.1.1.160"));
	//.subAggregation(AggregationBuilders.dateRange("dagg").field("bdate").format("yyyy-MM-dd||dd-MM-yyyy")	
	//		.addRange("1996-12-12", "12-12-1996"));
		builderNested.aggregation(aggregation);
		requestNested.source(builderNested);
		 
		try {
			SearchResponse scrollResp = client.search(requestNested, RequestOptions.DEFAULT);
			Nested agg = scrollResp.getAggregations().get("agg");
			Terms name = agg.getAggregations().get("agg");
			map.put("count", agg.getDocCount());
			ArrayList<Object> list = new ArrayList<>();
			for(SearchHit e : scrollResp.getHits().getHits())
			{
				list.add(e.getSourceAsMap());
				
			}
			map.put("response", list);
//			for (Terms.Bucket bucket : name.getBuckets()) {
//				ReverseNested resellerToProduct = bucket.getAggregations().get("OriginCityName");
//				map.put("counts", resellerToProduct.getDocCount());
//			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ResponseEntity.ok(map);	
	}
	// ........................ matchSearch.................................................. //

	public List<Student> matchSearch(HttpSession httpSession) {
		MultiSearchRequest request = new MultiSearchRequest();
		SearchRequest firstSearchRequest = new SearchRequest("jay");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("name", "JAY_SADAB")));

		// searchSourceBuilder.query(QueryBuilders.boolQuery().mustNot(QueryBuilders.matchQuery("name",
		// "Akash")));
		// searchSourceBuilder.query(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("age",
		// 20)));
//			searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("age").gt(22)));
		// searchSourceBuilder.query(QueryBuilders.boolQuery().filter(QueryBuilders.matchQuery("age",22)));
//			searchSourceBuilder.query(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("age", 20)));
//			searchSourceBuilder.query(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("age", 20)));
		firstSearchRequest.source(searchSourceBuilder);
		request.add(firstSearchRequest);
		try {

			MultiSearchResponse response = client.msearch(request, RequestOptions.DEFAULT);
			MultiSearchResponse.Item firstResponse = response.getResponses()[0];
			SearchResponse searchResponse = firstResponse.getResponse();
			MultiSearchResponse.Item secondResponse = response.getResponses()[0];
			searchResponse = secondResponse.getResponse();
			List list = new ArrayList();

			for (SearchHit s : searchResponse.getHits().getHits()) {
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
