package eu.deustotech.internet.dumper;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

/**
 * Hello world!
 *
 */
public class Main {
	
	private static SessionFactory sessionFactory;
	
	private void setUp() {
		
	}
	
    public static void main( String[] args )
    {
    	sessionFactory = new Configuration()
        .configure() // configures settings from hibernate.cfg.xml
        .buildSessionFactory();
    	
        System.out.println("Welcome to external SPARQL endpoint to Virtuoso dumper.");

    }
}
