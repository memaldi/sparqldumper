package eu.deustotech.internet.dumper.jobs;

import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import virtuoso.jena.driver.VirtGraph;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import eu.deustotech.internet.dumper.models.Settings;
import eu.deustotech.internet.dumper.models.Task;

public class LaunchJob implements Job {

	public static String TASK_ID = "TASK_ID";

	public void execute(JobExecutionContext context)
			throws JobExecutionException {

		JobDataMap data = context.getJobDetail().getJobDataMap();

		SessionFactory sessionFactory = new Configuration().configure() // configures settings
				// from
				// hibernate.cfg.xml
				.buildSessionFactory();

		Session session = sessionFactory.openSession();

		Task task = (Task) session.createQuery(
				"from Task as task where task.id=" + data.getLong(TASK_ID))
				.uniqueResult();
		if (task.getStatus().equals(Task.PAUSED)) {

			session.beginTransaction();
			task.setStart_time(new Date());
			task.setStatus(Task.RUNNING);
			session.update(task);
			session.getTransaction().commit();
			session.flush();
			
			Settings settings = (Settings) session.createQuery("from Settings")
					.list().get(0);

			String endpoint = task.getEndpoint();
			long offset = task.getOffset();
			boolean end = false;
			boolean paused = false;

			URI virtUri = null;
			try {
				virtUri = new URIBuilder().setScheme("jdbc:virtuoso")
						.setHost(settings.getHost())
						.setPort(Integer.parseInt(settings.getPort())).build();
			} catch (NumberFormatException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (URISyntaxException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			VirtGraph graph = new VirtGraph(task.getGraph(),
					virtUri.toString(), settings.getUser(),
					settings.getPassword());

			while (!end && !paused) {

				String query = "SELECT DISTINCT * WHERE {?s ?p ?o} LIMIT 1000 OFFSET "
						+ offset;

				CloseableHttpClient httpclient = HttpClients.createDefault();
				try {
					URI uri = new URIBuilder().setScheme("http")
							.setHost(endpoint.replace("http://", ""))
							.setParameter("query", query)
							.setParameter("output", "json")
							.setParameter("format", "json").build();
					HttpGet httpGet = new HttpGet(uri);
					CloseableHttpResponse response = httpclient
							.execute(httpGet);

					HttpEntity entity = response.getEntity();
					if (response.getStatusLine().getStatusCode() == 200) {
						StringWriter writer = new StringWriter();
						IOUtils.copy(entity.getContent(), writer);
						try {
							JSONObject jsonObj = new JSONObject(
									writer.toString());
							JSONArray bindings = jsonObj.getJSONObject(
									"results").getJSONArray("bindings");

							if (bindings.length() > 0) {

								for (int i = 0; i < bindings.length(); i++) {
									JSONObject item = bindings.getJSONObject(i);
									String s = item.getJSONObject("s")
											.getString("value");
									String p = item.getJSONObject("p")
											.getString("value");
									String o = item.getJSONObject("o")
											.getString("value");

									Node subject = Node.createURI(s);
									Node predicate = Node.createURI(p);
									Node object = null;

									if (item.getJSONObject("o")
											.getString("type").equals("uri")) {
										object = Node.createURI(o);

									} else {
										object = Node.createLiteral(o);
									}

									Triple triple = new Triple(subject,
											predicate, object);
									graph.add(triple);
								}
							} else {
								end = true;
								task.setEnd_time(new Date());
								task.setStatus(Task.DONE);
								session.beginTransaction();
								session.update(task);
								session.getTransaction().commit();
							}
						} catch (Exception e) {
							response.close();
							throw new Exception();
						}
					} else {
						task.setPaused_since(new Date());
						task.setOffset(offset);
						task.setStatus(Task.PAUSED);
						paused = true;
						session.beginTransaction();
						session.update(task);
						session.getTransaction().commit();
					}

					response.close();
				} catch (Exception e) {
					task.setPaused_since(new Date());
					task.setOffset(offset);
					task.setStatus(Task.PAUSED);
					paused = true;
					session.beginTransaction();
					session.update(task);
					session.getTransaction().commit();
				}
				offset += 1000;
			}
			graph.close();

		} else if (task.getStatus().equals(Task.DONE)) {
			Scheduler scheduler = context.getScheduler();
			try {
				scheduler.deleteJob(context.getJobDetail().getKey());
			} catch (SchedulerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
