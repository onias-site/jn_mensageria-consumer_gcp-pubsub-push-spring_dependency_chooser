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
@EnableAutoConfiguration(exclude={MongoAutoConfiguration.class})
@CrossOrigin
@RestController
@RequestMapping("/{topic}")
@SpringBootApplication
/**
 * Aplicação Spring Boot que recebe mensagens Pub/Sub push via endpoint REST {@code /{topic}}.
 * Inicializa as dependências de DI (Elasticsearch, Telegram, SendGrid, etc.) e delega o
 * processamento de cada mensagem ao {@code JnMensageriaReceiver}.
 */
public class JnMensageriaConsumerGcpPubSubPushSpringStarter {
	enum JsonFieldNames implements CcpJsonFieldName{
		message, localEnvironment
	}

	/**
	 * Mesma leitura feita por {@code CcpRestApiUtils.isLocalEnvironment()}, replicada aqui de
	 * propósito: depender de {@code ccp_rest-api-handler-exception_spring} só por este boolean
	 * traria actuator e log4j2 para dentro deste app e cruzaria duas versões de Spring Boot
	 * (aquele módulo usa 3.1.8, este usa 3.2.4).
	 */
	static boolean isLocalEnvironment() {
		CcpStringDecorator ccpStringDecorator = new CcpStringDecorator("application_properties");
		CcpPropertiesDecorator propertiesFrom = ccpStringDecorator.propertiesFrom();
		CcpJsonRepresentation systemProperties = propertiesFrom.environmentVariablesOrClassLoaderOrFile();
		boolean localEnvironment = systemProperties.getAsBoolean(JsonFieldNames.localEnvironment);
		return localEnvironment;
	}

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
	@PostMapping
	public void onReceiveMessage(@PathVariable("topic") String topic, @RequestBody Map<String, Object> body) {
		CcpJsonRepresentation CcpJsonRepresentation = new CcpJsonRepresentation(body);
		CcpJsonRepresentation internalMap = CcpJsonRepresentation.getInnerJson(JsonFieldNames.message);
		String data = internalMap.getAsString(JnEntityAsyncTask.Fields.data);
		CcpStringDecorator ccpStringDecorator = new CcpStringDecorator(data);
		CcpTextDecorator ccpStringDecoratorText = ccpStringDecorator.text();
		var asBase64 = ccpStringDecoratorText.asBase64();
		String str = asBase64.content;
		CcpJsonRepresentation json = new CcpJsonRepresentation(str);
		JnMensageriaReceiver.INSTANCE.executeProcess(
				JnEntityAsyncTask.ENTITY,  
				topic,  
				json  
				);
	}

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
