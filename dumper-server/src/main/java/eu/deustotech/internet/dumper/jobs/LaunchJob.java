package eu.deustotech.internet.dumper.jobs;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
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
import org.quartz.*;

import redis.clients.jedis.Jedis;
import virtuoso.jena.driver.VirtGraph;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import eu.deustotech.internet.dumper.models.Settings;
import eu.deustotech.internet.dumper.models.Task;

public class LaunchJob implements InterruptableJob {

	public static String TASK_ID = "TASK_ID";
	private boolean interrupted;
	
	public void execute(JobExecutionContext context)
			throws JobExecutionException {

		this.interrupted = false;
		
		JobDataMap data = context.getJobDetail().getJobDataMap();

		SessionFactory sessionFactory = new Configuration().configure() // configures settings
				// from
				// hibernate.cfg.xml
				.buildSessionFactory();
		Session session = sessionFactory.openSession();

        Jedis jedis = new Jedis("localhost");

        Task task = (Task) session.createQuery(
				"from Task as task where task.id=" + data.getLong(TASK_ID))
				.uniqueResult();



        if (jedis.get("dumper:job:" + task.getId()).equals(Task.PAUSED)) {

			session.beginTransaction();
			//task.setStart_time(new Date());
			//task.setStatus(Task.RUNNING);
            jedis.set("dumper:job:" + task.getId(), Task.RUNNING);
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

            CloseableHttpClient httpclient = HttpClients.createDefault();

            Properties prop = new Properties();
            InputStream input = null;

            try {
                //input = new FileInputStream("config.properties");
                input = getClass().getResourceAsStream("/config.properties");
                prop.load(input);
                input.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            RequestConfig config = RequestConfig.custom().build();
            if (prop.containsKey("proxy_host") && prop.containsKey("proxy_port") && prop.containsKey("proxy_protocol")) {
                HttpHost proxy = new HttpHost(prop.getProperty("proxy_host"), Integer.parseInt(prop.getProperty("proxy_port")), prop.getProperty("proxy_protocol"));
                config = RequestConfig.custom().setProxy(proxy).build();
            }


			while (!end && !paused && !this.interrupted) {

				String query = "SELECT DISTINCT * WHERE {?s ?p ?o} LIMIT 1000 OFFSET "
						+ offset;


				try {
					URI uri = new URIBuilder().setScheme("http")
							.setHost(endpoint.replace("http://", ""))
							.setParameter("query", query)
							.setParameter("output", "json")
							.setParameter("format", "json").build();
					HttpGet httpGet = new HttpGet(uri);
                    httpGet.setConfig(config);
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
								//task.setStatus(Task.DONE);
                                jedis.set("dumper:job:" + task.getId(), Task.DONE);
								session.beginTransaction();
								session.update(task);
								session.getTransaction().commit();
							}
						} catch (Exception e) {
                            jedis.set("dumper:job:" + task.getId(), Task.PAUSED);
							response.close();
							throw new Exception();
						}
					} else {
						task.setPaused_since(new Date());
						task.setOffset(offset);
						//task.setStatus(Task.PAUSED);
                        jedis.set("dumper:job:" + task.getId(), Task.PAUSED);
						paused = true;
						session.beginTransaction();
						session.update(task);
						session.getTransaction().commit();
					}

					response.close();
				} catch (Exception e) {
                    e.printStackTrace();
					task.setPaused_since(new Date());
					task.setOffset(offset);
					//task.setStatus(Task.PAUSED);
                    jedis.set("dumper:job:" + task.getId(), Task.PAUSED);
					paused = true;
					session.beginTransaction();
					session.update(task);
					session.getTransaction().commit();
				}
				offset += 1000;
                session.beginTransaction();
                task.setOffset(offset);
                session.update(task);
                session.getTransaction().commit();
			}
			graph.close();

		} else if (jedis.get("dumper:job:" + task.getId()).equals(Task.DONE)) {
			Scheduler scheduler = context.getScheduler();
			try {
				scheduler.deleteJob(context.getJobDetail().getKey());
			} catch (SchedulerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void interrupt() throws UnableToInterruptJobException {
		this.interrupted = true;
	}
}
