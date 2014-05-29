package eu.deustotech.internet.dumper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.List;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import eu.deustotech.internet.dumper.models.Settings;
import eu.deustotech.internet.dumper.models.Task;

/**
 * Hello world!
 * 
 */
public class Main {

	private static SessionFactory sessionFactory;
	private static BufferedReader in = new BufferedReader(
			new InputStreamReader(System.in));

	public static void main(String[] args) {
		sessionFactory = new Configuration().configure() // configures settings
															// from
															// hibernate.cfg.xml
				.buildSessionFactory();

		System.out
				.println("Welcome to external SPARQL endpoint to Virtuoso dumper.");

		Session session = sessionFactory.openSession();
		List<Settings> settings_list = (List<Settings>) session.createQuery(
				"from Settings").list();
		Settings settings = null;
		if (settings_list.size() <= 0) {
			try {
				System.out
						.println("It seems that is the first time that you use this tool. Pleas fulfill the following config parameters:");
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

		boolean exit = false;

		while (!exit) {
			System.out.println("a) Create a new dump task");
			System.out.println("b) Delete all tasks");
			System.out.println("c) Show tasks");
			System.out.println("d) Resume task");
			System.out.println("e) Exit");
			System.out.print("Select your choice: ");
			try {
				String option = in.readLine();
				switch (option) {
				case "a":
					create_task();
					break;
				case "e":
					exit = true;
					break;
				default:
					System.out.println("Wrong option!");
					break;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		System.out.println("Bye!");
		session.close();

	}

	private static void show_tasks() {
		Session session = sessionFactory.openSession();

		List<Task> task_list = session.createQuery("from Task").list();
		for (Task task : task_list) {
			System.out
					.println("id | SPARQL endpoint | Named graph | Status | Start time | End time | Paused since | Offset");
			System.out.format("%l | %s | %s | %s | %s | %s | %s | %l",
					task.getId(), task.getEndpoint(), task.getGraph(),
					task.getStatus(), task.getStart_time(), task.getEnd_time(),
					task.getPaused_since(), task.getOffset());
		}
		session.close();
	}

	private static void create_task() {
		try {

			Session session = sessionFactory.openSession();

			System.out.println("Creating a new task:");
			System.out.print("Source SPARQL endpoint: ");
			String endpoint = in.readLine();
			System.out
					.print("Named graph from Virtuoso in which data is going to be stored: ");
			String graph = in.readLine();

			Task task = new Task();
			task.setEndpoint(endpoint);
			task.setGraph(graph);
			task.setOffset((long) 0);
			task.setStart_time(new Date());

			session.beginTransaction();
			session.save(task);

			session.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
