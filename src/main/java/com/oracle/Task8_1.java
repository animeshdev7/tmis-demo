package com.oracle;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jdbc.JdbcComponent;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import com.oracle.util.MessageProcessor;

@SpringBootApplication
public class Task8_1 {
	public static void main(String[] args) throws Exception {
		CamelContext context = new DefaultCamelContext();
		MQConnectionFactory mqCF = new MQConnectionFactory();

		mqCF.setHostName("");
		mqCF.setPort();
		mqCF.setQueueManager("");
		mqCF.setChannel("");
		mqCF.setTransportType(WMQConstants.WMQ_CM_CLIENT);

		mqCF.setStringProperty(WMQConstants.USERID, "");
		mqCF.setStringProperty(WMQConstants.PASSWORD, "");
		mqCF.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);

		context.addComponent("jms", JmsComponent.jmsComponentAutoAcknowledge(mqCF));
		MessageProcessor messageProcessor = new MessageProcessor();

		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName("oracle.jdbc.OracleDriver");
		ds.setUrl("jdbc:oracle:thin:@localhost:1521:FREE");
		ds.setUsername("SYSTEM");
		ds.setPassword("1149135198Dev");

		JdbcComponent jdbcComponent = new JdbcComponent();
		jdbcComponent.setDataSource(ds);
		context.addComponent("jdbc", jdbcComponent);
		context.getRegistry().bind("dataSource", ds);

		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() throws Exception {

//	

				from("file-watch:C:/Users/Animesh/Desktop/Training_op/Testing/input" + "?events=MODIFY"
						+ "&recursive=false").filter(header("CamelFileName").isEqualTo("testfile1.txt"))
						.routeId("stage-0").log("[Step - 1.0] Application Started")

						.convertBodyTo(String.class).log("[Step - 1.1] Pushing message to the input queue.")
						.to("jms:queue:input_queue").log("[Step - 1.1] Message sent to the input queue.");

//

				from("jms:queue:input_queue").routeId("stage-1").log("[Step - 2.1] Recieved message from input queue")
						.process(exec -> {
							String jsonPayload = exec.getIn().getBody().toString();
							exec.setProperty("headers", exec.getIn().getHeaders());
							messageProcessor.receiveValidateTransform(jsonPayload, exec);
						})
						.log("[Step - 3.1] Created tmis entry-> tmisId=${header.tmisId}, correlationId=${header.correlationId}, payload=${header.payload}")
						.setBody(simple(
								"Insert into tmisaudit (tmisId, correlationId, payload, sentToTarget, createdDate) VALUES ('${header.tmisId}', '${header.correlationId}', '${header.payload}', 'P', SYSTIMESTAMP)"))
						.to("jdbc:dataSource?useHeadersAsParameters=true").process(exchange -> {
							String jdwsonPayload = (String) exchange.getProperty("payload");
							System.out.println(exchange.getIn().getHeader("payload"));
						}).log("[Step - 3.2] Data inserted in audit table with status 'P'")

						.process(exchange -> {
							String jsonPayload = (String) exchange.getProperty("payload");
							exchange.getIn().setBody(jsonPayload);
						})

//

						.doTry().log("[Step - 4.1] Sending tranformed data to output queue")
//					.process(exchange -> {
//					    if (true) {
//					        throw new RuntimeException("Simulated forward failure");
//					    }
//					})	
						.to("jms:queue:output_queue").log("[Step - 4.2] Tranformed data sent to output queue")

						.setBody(simple(
								"update tmisaudit set sentToTarget = 'Y' where tmisId = '${header.tmisId}' AND correlationId = '${header.correlationId}'"))
						.to("jdbc:dataSource?useHeadersAsParameters=true")
						.log("[Step - 4.3] Data updated in audit table with status 'Y'")
						.log("[Step - 5] Process executed successfully!")

						.doCatch(Exception.class)
						.log("[Step - 4.2] There was error while sending data to the output queue!")
						.setBody(simple(
								"update tmisaudit set sentToTarget = 'E' where tmisId = '${header.tmisId}' AND correlationId = '${header.correlationId}'"))
						.to("jdbc:dataSource?useHeadersAsParameters=true")
						.log("[Step - 4.3] Data updated in audit table with status 'E'")

						.log("[Step - 5] Process executed successfully!").end();

			}
		});

		context.start();
		Thread.currentThread().join();
		context.stop();
	}
}
