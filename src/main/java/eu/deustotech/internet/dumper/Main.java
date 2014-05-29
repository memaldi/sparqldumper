package eu.deustotech.internet.dumper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import eu.deustotech.internet.dumper.models.Settings;

/**
 * Hello world!
 *
 */
public class Main {
	
	private static SessionFactory sessionFactory;
	private static BufferedReader in = new BufferedReader(new InputStreamReader(System.in)); 
	
	private void setUp() {
		
	}
	
    public static void main( String[] args )
    {
    	sessionFactory = new Configuration()
        .configure() // configures settings from hibernate.cfg.xml
        .buildSessionFactory();
    	
        System.out.println("Welcome to external SPARQL endpoint to Virtuoso dumper.");
        
        Session session = sessionFactory.openSession();
        List<Settings> settings_list = (List<Settings>) session.createQuery("from Settings").list();
        Settings settings = null;
        if (settings_list.size() <= 0) {
        	try {
        		System.out.println("It seems that is the first time that you use this tool. Pleas fulfill the following config parameters:");
            	System.out.print("Virtuoso host: ");
            	String virtuoso_host = in.readLine();
            	System.out.print("Virtuoso port: ");
				String virtuoso_port = in.readLine();
				System.out.print("Virtuoso user: ");
				String virtuoso_user = in.readLine();
				System.out.print("Virtuoso password: ");
				String virtuoso_password = in.readLine();
				
				settings = new Settings();
				settings.setHost(virtuoso_host);
				settings.setPort(virtuoso_port);
				settings.setUser(virtuoso_user);
				settings.setPassword(virtuoso_password);
				
				session.beginTransaction();
				session.save(settings);
				session.getTransaction().commit();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        } else {
        	settings = settings_list.get(0);
        }
        
        session.close();

    }
}
