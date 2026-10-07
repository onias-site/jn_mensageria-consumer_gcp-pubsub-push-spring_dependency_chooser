package com.jn.topic.consumer.pubsub.push.application;


import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpPropertiesDecorator;
import com.ccp.decorators.CcpStringDecorator;
import com.ccp.decorators.CcpTextDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.implementations.db.bulk.elasticsearch.CcpElasticSerchDbBulk;
import com.ccp.implementations.db.crud.elasticsearch.CcpElasticSearchCrud;
import com.ccp.implementations.db.query.elasticsearch.CcpElasticSearchQueryExecutor;
import com.ccp.implementations.db.utils.elasticsearch.CcpElasticSearchDbRequest;
import com.ccp.implementations.email.sendgrid.CcpSendGridEmailSender;
import com.ccp.implementations.file.bucket.gcp.CcpGcpFileBucket;
import com.ccp.implementations.http.apache.mime.CcpApacheMimeHttp;
import com.ccp.implementations.instant.messenger.telegram.CcpTelegramInstantMessenger;
import com.ccp.implementations.json.gson.CcpGsonJsonHandler;
import com.ccp.implementations.mensageria.sender.gcp.pubsub.CcpGcpPubSubMensageriaSender;
import com.ccp.local.testings.implementations.CcpLocalInstances;
import com.jn.entities.JnEntityAsyncTask;
import com.jn.mensageria.JnMensageriaReceiver;
/**
 * Spring Boot application that receives the Pub/Sub push messages at {@code /{topic}}. It wires the dependencies
 * (Elasticsearch, Telegram, SendGrid and so on) and hands each message to {@code JnMensageriaReceiver}.
 */
@EnableAutoConfiguration(exclude={MongoAutoConfiguration.class})
@CrossOrigin
@RestController
@RequestMapping("/{topic}")
@SpringBootApplication
public class JnMensageriaConsumerGcpPubSubPushSpringStarter {
	/** Fields read by the consumer. */
	enum JsonFieldNames implements CcpJsonFieldName{
		/** The Pub/Sub envelope of the pushed message. */
		message,
		/** The system property that tells whether the system runs on a developer machine. */
		localEnvironment
	}

	/**
	 * The same reading done by {@code CcpRestApiUtils.isLocalEnvironment()}, repeated here on purpose: depending on
	 * {@code ccp_rest-api-handler-exception_spring} only for this boolean would bring actuator and log4j2 into this app and
	 * mix two Spring Boot versions (that module uses 3.1.8, this one 3.2.4).
	 * @return the {@code localEnvironment} system property
	 */
	static boolean isLocalEnvironment() {
		CcpStringDecorator ccpStringDecorator = new CcpStringDecorator("application_properties");
		CcpPropertiesDecorator propertiesFrom = ccpStringDecorator.propertiesFrom();
		CcpJsonRepresentation systemProperties = propertiesFrom.environmentVariablesOrClassLoaderOrFile();
		boolean localEnvironment = systemProperties.getAsBoolean(JsonFieldNames.localEnvironment);
		return localEnvironment;
	}

	/**
	 * Wires the dependencies and starts Spring.
	 * @param args the command line arguments
	 */
	public static void main(String[] args) {
		boolean localEnvironment = isLocalEnvironment();
		CcpElasticSearchQueryExecutor ccpElasticSearchQueryExecutor = new CcpElasticSearchQueryExecutor();
		CcpTelegramInstantMessenger ccpTelegramInstantMessenger = new CcpTelegramInstantMessenger();
		CcpElasticSearchDbRequest ccpElasticSearchDbRequest = new CcpElasticSearchDbRequest();
		CcpSendGridEmailSender ccpSendGridEmailSender = new CcpSendGridEmailSender();
		CcpElasticSerchDbBulk ccpElasticSerchDbBulk = new CcpElasticSerchDbBulk();
		CcpElasticSearchCrud ccpElasticSearchCrud = new CcpElasticSearchCrud();
		CcpGsonJsonHandler ccpGsonJsonHandler = new CcpGsonJsonHandler();
		CcpApacheMimeHttp ccpApacheMimeHttp = new CcpApacheMimeHttp();
		CcpGcpFileBucket ccpGcpFileBucket = new CcpGcpFileBucket();
		CcpDependencyInjection.loadAllDependencies(
				localEnvironment ? CcpLocalInstances.syncMensageriaListener : new CcpGcpPubSubMensageriaSender(),
				ccpElasticSearchQueryExecutor,
				ccpTelegramInstantMessenger,
				ccpElasticSearchDbRequest,
				ccpSendGridEmailSender,
				ccpElasticSerchDbBulk,
				ccpElasticSearchCrud,
				ccpGsonJsonHandler,
				ccpApacheMimeHttp,
				ccpGcpFileBucket  
				);
		SpringApplication.run(JnMensageriaConsumerGcpPubSubPushSpringStarter.class, args);
	}
	/**
	 * Receives a pushed message and runs the task of the topic.
	 * @param topic the topic
	 * @param body the Pub/Sub push request
	 */
	@PostMapping
	public void onReceiveMessage(@PathVariable("topic") String topic, @RequestBody Map<String, Object> body) {
		CcpJsonRepresentation json = getMessage(body);
		JnMensageriaReceiver.INSTANCE.executeProcess(
				JnEntityAsyncTask.ENTITY,  
				topic,  
				json  
				);
	}

	/**
	 * Extracts the message of a Pub/Sub push request: {@code message.data} comes in Base64 (the publisher sends it in Base64
	 * through the REST api, and the push delivers it the same way), so it is decoded, as UTF-8, before becoming JSON. Until
	 * 2026-10-06 the data was encoded to Base64 once more instead of decoded, and every pushed message failed to parse.
	 * @param body the Pub/Sub push request
	 * @return the message
	 */
	static CcpJsonRepresentation getMessage(Map<String, Object> body) {
		CcpJsonRepresentation pushRequest = new CcpJsonRepresentation(body);
		CcpJsonRepresentation internalMap = pushRequest.getInnerJson(JsonFieldNames.message);
		String data = internalMap.getAsString(JnEntityAsyncTask.Fields.data);
		CcpStringDecorator ccpStringDecorator = new CcpStringDecorator(data);
		CcpTextDecorator ccpStringDecoratorText = ccpStringDecorator.text();
		CcpTextDecorator decodedData = ccpStringDecoratorText.fromBase64();
		String messageAsText = decodedData.content;
		CcpJsonRepresentation json = new CcpJsonRepresentation(messageAsText);
		return json;
	}

	/**
	 * Runs the task of the topic with a JSON sent as is, without the Pub/Sub envelope (for tests).
	 * @param topic the topic
	 * @param json the message
	 */
	@PostMapping("/testing")
	public void onReceiveMessageTesting(@PathVariable("topic") String topic, @RequestBody Map<String, Object> json) {
		CcpJsonRepresentation md = new CcpJsonRepresentation(json);
		JnMensageriaReceiver.INSTANCE.executeProcess(
				JnEntityAsyncTask.ENTITY,  
				topic,  
				md 
				);
	}

}
