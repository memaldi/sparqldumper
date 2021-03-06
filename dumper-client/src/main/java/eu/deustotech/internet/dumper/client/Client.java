package eu.deustotech.internet.dumper.client;

import eu.deustotech.internet.dumper.jobs.LaunchJob;
import eu.deustotech.internet.dumper.models.Settings;
import eu.deustotech.internet.dumper.models.Task;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Hello world!
 * 
 */
public class Client {

	private static SessionFactory sessionFactory;
	private static Session session;
	private static BufferedReader in = new BufferedReader(
			new InputStreamReader(System.in));

	private static SchedulerFactory sf = new StdSchedulerFactory();
    private static Jedis jedis = new Jedis("localhost");


    public static void main(String[] args) {
		sessionFactory = new Configuration().configure() // configures settings
															// from
															// hibernate.cfg.xml
				.buildSessionFactory();

		session = sessionFactory.openSession();
		System.out
				.println("Welcome to external SPARQL endpoint to Virtuoso dumper.");

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
			session.clear();
			System.out.println("a) Create a new dump task");
			System.out.println("b) Delete all tasks");
			System.out.println("c) Show tasks");
			System.out.println("d) Stop task");
			System.out.println("e) Resume tasks");
			System.out.println("f) Exit");
			System.out.print("Select your choice: ");
			try {
				String option = in.readLine();
				switch (option.toLowerCase()) {
				case "a":
					create_task();
					break;
				case "b":
					delete_tasks();
					break;
				case "c":
					show_tasks();
					break;
				case "d":
					stop_task();
					break;
                case "e":
                    resume_tasks();
                    break;
				case "f":
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
		System.exit(0);
	}

    private static void resume_tasks() {
        Logger logger = Logger.getLogger(Client.class.getName());
        Set<String> keySet = jedis.keys("dumper:job:*");

        for (String key : keySet) {
            String status = jedis.get(key);
            if (status.equals(Task.PAUSED)) {
                String taskId = key.split(":")[2];
                logger.info(String.format("Relaunching task %s", taskId));
                JobDetail job = JobBuilder
                        .newJob(LaunchJob.class)
                        .withIdentity("launchJob-" + taskId,
                                "dumper").build();
                Trigger trigger = TriggerBuilder
                        .newTrigger()
                        .withIdentity("trigger-" + taskId,
                                "dumper")
                        .startNow()
                        .withSchedule(
                                SimpleScheduleBuilder.simpleSchedule()
                                        .withIntervalInMinutes(15).repeatForever())
                        .build();

                job.getJobDataMap().put(LaunchJob.TASK_ID, taskId);

                Scheduler sched = null;
                try {
                    sched = sf.getScheduler();
                    sched.scheduleJob(job, trigger);
                } catch (SchedulerException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void stop_task() {
		try {
			System.out.print("Job ID: ");
			String id = in.readLine();
			Scheduler sched = sf.getScheduler();
			JobKey jobKey = new JobKey("launchJob-" + id, "dumper");
			sched.interrupt(jobKey);
			sched.deleteJob(jobKey);
			Task task = (Task) session.createQuery("from Task as task where task.id=" + id).uniqueResult();
			session.beginTransaction();
			session.delete(task);
			session.getTransaction().commit();
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		
	}

	private static void delete_tasks() {
		List<Task> taskList = session.createQuery("from Task").list();
		session.beginTransaction();
		for (Task task : taskList) {
			session.delete(task);
		}
		session.getTransaction().commit();
	}

	private static void show_tasks() {
		session.beginTransaction();
		List<Task> task_list = session.createQuery("from Task").list();
		session.getTransaction().commit();
		System.out
				.println("id | SPARQL endpoint | Named graph | Status | Start time | End time | Paused since | Offset");
		for (Task task : task_list) {
			System.out.format("%s | %s | %s | %s | %s | %s | %s | %s", task
					.getId().toString(), task.getEndpoint(), task.getGraph(),
					jedis.get("dumper:job:" + task.getId()), task.getStart_time(), task.getEnd_time(),
					task.getPaused_since(), task.getOffset().toString());
			System.out.println();
		}
	}

	private static void create_task() {
		try {
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
			//task.setStatus(Task.PAUSED);


			session.beginTransaction();
			session.save(task);
			session.getTransaction().commit();

            jedis.set("dumper:job:" + task.getId(), Task.PAUSED);

			JobDetail job = JobBuilder
					.newJob(LaunchJob.class)
					.withIdentity("launchJob-" + task.getId().toString(),
							"dumper").build();
			Trigger trigger = TriggerBuilder
					.newTrigger()
					.withIdentity("trigger-" + task.getId().toString(),
							"dumper")
					.startNow()
					.withSchedule(
							SimpleScheduleBuilder.simpleSchedule()
									.withIntervalInMinutes(15).repeatForever())
					.build();

			job.getJobDataMap().put(LaunchJob.TASK_ID, task.getId());

			Scheduler sched = sf.getScheduler();
			sched.scheduleJob(job, trigger);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
