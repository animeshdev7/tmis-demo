package com.oracle.util;

import java.lang.foreign.ValueLayout;
import java.time.Instant;
import java.util.UUID;

import org.apache.camel.Exchange;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageProcessor {

	private final ObjectMapper mapper = new ObjectMapper();
//	private final TmisAuditDao auditDao = new TmisAuditDao();
	
	private static final String[] required_fields = {
		    "StudentID",
		    "StudentName",
		    "age",
		    "AddressLine1",
		    "AddressLine2",
		    "Country"
		};
	
	private static final String[] address_fields = {
			"AddressLine1",
			"AddressLine2",
			"Town",
			"Country",
			"Pincode"
	};

	

	public void receiveValidateTransform(String message, Exchange exchange) throws Exception {

		if (message == null) {
			log.info("[Step - 2.2] No message received from the queue");
			return;
		}

		log.info("[Step - 2.2] : Message retrieved from input queue");
		JsonNode root;
		
		if(message.trim().startsWith("<")) {
			XmlMapper xmlMapper = new XmlMapper();
			root = xmlMapper.readTree(message.getBytes());
			log.info("[Step - 2.2.2] The message received was in xml format: "+xmlMapper.writerWithDefaultPrettyPrinter().writeValueAsString(root));
		}else {
			root = mapper.readTree(message);
			log.info("[Step - 2.2.2] The message received was in json format: "+mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root));
		}

		validate(root);

		String payload = transform(root);
		
		String tmisId = generateTmisId();
		String correlationId = exchange.getIn().getHeader("JMSMessageID").toString();
		log.info("[Step - 2.6] : TmisId and CorrelateionId generated");
		
		exchange.getIn().setHeader("tmisId",tmisId);
		exchange.getIn().setHeader("correlationId", correlationId);
		exchange.getIn().setHeader("payload", payload);
	}

	private void validate(JsonNode root) {
		
			log.info("[Step - 2.3] : Validating fields");
			for(String field: required_fields) {
				require(root, field);
			}
			log.info("[Step - 2.4] : Validation of fields successful!");
	}

	private void require(JsonNode root, String field) {
		if (!root.hasNonNull(field)) {
			log.error("[Step - 2.4] : Validation of fields failed!");
			log.info("[Step - 2.4] : Missing required field: "+field);
			throw new IllegalArgumentException("Missing required field: " + field); 
		}
	}

	private String transform(JsonNode root) throws Exception {

		ObjectNode newJson = mapper.createObjectNode();

		ArrayNode fullAddress = mapper.createArrayNode();
		for(String field: address_fields) {
			JsonNode node = root.get(field);
			if(node != null && !node.isNull()) {
				String val = node.asText(); 
				if(!val.isEmpty()) {					
					fullAddress.add(val);
				}
			}
		}

		newJson.set("StudentID", root.get("StudentID"));
		newJson.set("StudentName", root.get("StudentName"));
		newJson.set("age", root.get("age"));
		newJson.set("AddressLine", fullAddress);
		
		String newJsonString = mapper.writeValueAsString(newJson);
		log.info("[Step - 2.5] : Message transformation successful, transformed json msg: "+ mapper.writerWithDefaultPrettyPrinter()
        .writeValueAsString(newJson));
		return newJsonString;
	}
	
	 public String generateTmisId() {
	        return "TMIS-" + Instant.now().toEpochMilli();
	    }

	    public static String generateCorrelationId() {
	        return UUID.randomUUID().toString();
	    }
}
