package edu.sjsu.cmpe.procurement.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;
import edu.sjsu.cmpe.procurement.ProcurementService;

import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.sound.midi.Receiver;
import javax.ws.rs.core.MediaType;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;

import org.json.JSONObject;
import org.json.JSONArray;

import edu.sjsu.cmpe.procurement.ProcurementService;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;


/**
 * This job will run at every 300 second.
 */
@Every("300s")
public class ProcurementSchedulerJob extends Job 
{
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void doJob() 
    {
    	try
    	{    	
    	
    	String user = env("APOLLO_USER", "admin");
		String password = env("APOLLO_PASSWORD", "password");
		String host = env("APOLLO_HOST", "54.215.133.131");
		int port = Integer.parseInt(env("APOLLO_PORT", "61613"));
		String destination = "/queue/59198.book.orders";
	
		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + host + ":" + port);

		Connection connection = factory.createConnection(user, password);
		connection.start();
		
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = new StompJmsDestination(destination);
		ArrayList<Integer> isbn = new ArrayList<Integer>();
	
		MessageConsumer consumer = session.createConsumer(dest);
		System.out.println("Waiting for messages from " + "/queue/59198.book.orders" + "...");
	
		long waitUntil = 5000; // wait for 5 sec
		while(true) 
		{
			Message msg = consumer.receive(waitUntil);
			if( msg instanceof  TextMessage ) 
			{
				String body = ((TextMessage) msg).getText();
				System.out.println("Received message = " + body);
				String temp[] = body.split("[:]");
				isbn.add(Integer.parseInt(temp[1]));
			} 
			else if (msg == null) 
			{
				System.out.println("No new messages. Exiting due to timeout - " + waitUntil / 1000 + " sec");
				break;	
			} 
			else 
			{
				System.out.println("Unexpected message type: "+msg.getClass());
			}   
		}
		//POST to Publisher
		if(isbn.size() > 0)
		{
			Client c = Client.create();
			String message = "{\"id\":\"59198\",\"order_book_isbns\":" + isbn + "}";
			WebResource webResource_1 = c.resource("http://54.215.133.131:9000/orders");
			ClientResponse response_1 = webResource_1.type("application/json").post(ClientResponse.class, message);
			if (response_1.getStatus()!=200) 
			{
				throw new RuntimeException("Failed: HTTP error : " +response_1.getStatus());	
			}
			System.out.println(response_1.toString());
			System.out.println(response_1.getEntity(String.class));
		}
	
			
		//GET from Publisher
		Client c = Client.create();
		WebResource webResource_2 = c.resource("http://54.215.133.131:9000/orders/59198");
		ClientResponse response_2 = webResource_2.accept("application/json").get(ClientResponse.class);
		if (response_2.getStatus() != 200) {
			throw new RuntimeException("Failed: HTTP error : " +response_2.getStatus());
		}
		System.out.println(response_2.toString());
		String server_books = response_2.getEntity(String.class);
		System.out.println(server_books);
		System.out.println("\n");
		
		JSONObject object = new JSONObject(server_books);
		JSONArray books_shipped = object.getJSONArray("shipped_books");
		int length = books_shipped.length();
		String parse_books = ""; 
		String topicQ = "/topic/59198.book.";
		
		for (int i=0; i<length; i++)
		{
			Long isbn_num = books_shipped.getJSONObject(i).getLong("isbn");
			String title = books_shipped.getJSONObject(i).getString("title");
			String category = books_shipped.getJSONObject(i).getString("category");
			String cover_image = books_shipped.getJSONObject(i).getString("coverimage");
			parse_books = isbn_num + ":\"" + title + "\":\"" + category + "\":\"" + cover_image + "\"";
			String dest_topic = topicQ + category;
			      
			Session session_2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination topic_dest = new StompJmsDestination(dest_topic);
			           
			MessageProducer producer_2 = session_2.createProducer(topic_dest);
			producer_2.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

			TextMessage msg = session_2.createTextMessage(parse_books);
			msg.setLongProperty("id", System.currentTimeMillis());

			
			producer_2.send(msg);
			System.out.println("Topic created." + msg);
			
			
		}
		
    
    	connection.close();	
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}

}
    
    private static String env(String key, String defaultValue) 
    {
    	String rc = System.getenv(key);
    	if( rc== null ) {
    	    return defaultValue;
    	}
    	return rc;
    }

    private static String arg(String []args, int index, String defaultValue) 
    {
    	if( index < args.length ) {
    	    return args[index];
    	} else {
    	    return defaultValue;
    	}
    }
    
    
}
