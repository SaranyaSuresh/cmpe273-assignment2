package edu.sjsu.cmpe.library;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.assets.AssetsBundle;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.views.ViewBundle;

import edu.sjsu.cmpe.library.api.resources.BookResource;
import edu.sjsu.cmpe.library.api.resources.RootResource;
import edu.sjsu.cmpe.library.config.LibraryServiceConfiguration;
import edu.sjsu.cmpe.library.repository.BookRepository;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;
import edu.sjsu.cmpe.library.ui.resources.HomeResource;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;

import edu.sjsu.cmpe.library.domain.Book;
import edu.sjsu.cmpe.library.domain.Book.Status;
import edu.sjsu.cmpe.library.dto.BookDto;
import edu.sjsu.cmpe.library.repository.BookRepository;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LibraryService extends Service<LibraryServiceConfiguration> {

    private final Logger log = LoggerFactory.getLogger(getClass());
    public static String aUser;
    public static String aPassword;
    public static String aHost;
    public static int aPort;
    public static String dest;
    public static BookRepository bookRepository;

    public static void main(String[] args) throws Exception {
	new LibraryService().run(args);
	
	int num_of_threads = 2;
	ExecutorService execute = Executors.newFixedThreadPool(num_of_threads);
	Runnable bg_thread = new Runnable()
	{
		@Override
		public void run()
		{
			listener();
		
		}
	};
	execute.execute(bg_thread);
}
	
    @Override
    public void initialize(Bootstrap<LibraryServiceConfiguration> bootstrap) {
	bootstrap.setName("library-service");
	bootstrap.addBundle(new ViewBundle());
	bootstrap.addBundle(new AssetsBundle());
    }

    @Override
    public void run(LibraryServiceConfiguration configuration,
	    Environment environment) throws Exception {
	// This is how you pull the configurations from library_x_config.yml
	String queueName = configuration.getStompQueueName();
	String topicName = configuration.getStompTopicName();
	String libraryName = configuration.getLibraryName();
	String apolloUser = configuration.getApolloUser();
	String apolloPassword = configuration.getApolloPassword();
	String apolloHost = configuration.getApolloHost();
	String apolloPort = configuration.getApolloPort();
	log.debug("{} - Queue name is {}. Topic name is {}",
		configuration.getLibraryName(), queueName,
		topicName);
	// TODO: Apollo STOMP Broker URL and login
	aUser = env("APOLLO_USER", apolloUser);
	aPassword = env("APOLLO_PASSWORD", apolloPassword);
	aHost = env("APOLLO_HOST", apolloHost);
	aPort = Integer.parseInt(env("APOLLO_PORT", apolloPort));
	dest = topicName;
	
	/** Root API */
	environment.addResource(RootResource.class);
	/** Books APIs */
	bookRepository = new BookRepository();
	environment.addResource(new BookResource(bookRepository, libraryName, topicName, queueName, apolloUser, apolloPassword, apolloHost, apolloPort));

	/** UI Resources */
	environment.addResource(new HomeResource(bookRepository));
    }
    
    public static void listener()
    {
    	try
    	{
    	StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
    	factory.setBrokerURI("tcp://" + aHost + ":" + aPort);
    	Connection connection = factory.createConnection(aUser, aPassword);
    	connection.start();
    	Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    	Destination destiny = new StompJmsDestination(dest);
    	
    	MessageConsumer consumer = session.createConsumer(destiny);
    	System.currentTimeMillis();
    	System.out.println("Waiting for messages...");
    	while(true)
    	{
    		Message msg = consumer.receive();
    	    if( msg instanceof  TextMessage ) 
    	    {
	    		String body = ((TextMessage) msg).getText();
	    		System.out.println("Received message = " + body);
	    		String[] contents = body.split(":",4);
	    		Long isbn = Long.parseLong(contents[0]);
				String title = contents[1].substring(1, contents[1].length()-1);
				String category = contents[2].substring(1, contents[2].length()-1);
				String coverimage = contents[3].substring(1, contents[3].length()-1);
				  
				Book book = new Book();
				System.out.println("isbn is: "+ isbn);
				book.setIsbn(isbn);
				System.out.println("Title is: "+title);
	    		book.setTitle(title);
	    		System.out.println("Category is: " + category);
	    		book.setCategory(category);
	    		String trimmed_url = coverimage.replaceAll("^\"|\"$", "");
	    		URL url = new URL(trimmed_url);
	    		System.out.println("Coverimage is: "+ url);
				book.setCoverimage(url);
								
	    		bookRepository.saveBook(book);
	    		System.out.println("Book creation successful.");
	    			    		
	    		if(bookRepository.getBookByISBN(book.getIsbn()).getStatus().toString() == "lost")
                {
                        bookRepository.getBookByISBN(book.getIsbn()).setStatus(Status.available);
                        System.out.println("Book updated with status as 'available'.");
                        
                }
	    		
 
	    	}
    	    else if (msg instanceof StompJmsMessage) 
    	    {
    			StompJmsMessage smsg = ((StompJmsMessage) msg);
    			String body = smsg.getFrame().contentAsString();
    			if ("SHUTDOWN".equals(body)) {
    			    break;
    			}
    			System.out.println("Received message = " + body);

    		} 
    	    else 
    	    {
    			System.out.println("Unexpected message type: "+msg.getClass());
    		}
    	}
    	connection.close();
    	
     	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    }
    	
   
    private static String env(String key, String defaultValue) {
    	String rc = System.getenv(key);
    	if( rc== null ) {
    	    return defaultValue;
    	}
    	return rc;
    }
}
