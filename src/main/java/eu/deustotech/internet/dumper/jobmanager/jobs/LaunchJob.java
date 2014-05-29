package eu.deustotech.internet.dumper.jobmanager.jobs;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.hibernate.Session;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import eu.deustotech.internet.dumper.models.Task;

public class LaunchJob implements Job {

	public static String TASK_ID = "TASK_ID";
	public static String SESSION = "SESSION";

	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		// TODO Auto-generated method stub
		JobDataMap data = context.getJobDetail().getJobDataMap();
		Session session = (Session) data.get(SESSION);
		Task task = (Task) session.createQuery(
				"from Task as task where task.id=" + data.getLong(TASK_ID))
				.uniqueResult();
		session.beginTransaction();
		task.setStart_time(new Date());
		task.setStatus("RUNNING");
		session.update(task);
		session.getTransaction().commit();

		String endpoint = task.getEndpoint();
		long offset = task.getOffset();
		boolean end = false;
		while (!end) {
			String query = "SELECT DISTINCT * WHERE {?s ?p ?o} LIMIT 1000 OFFSET " + offset;
	
			CloseableHttpClient httpclient = HttpClients.createDefault();
			try {
				URI uri = new URIBuilder().setScheme("http").setHost(endpoint.replace("http://", ""))
						.setParameter("query", query).setParameter("output", "json")
						.setParameter("format", "json").build();
				HttpGet httpGet = new HttpGet(uri);
				CloseableHttpResponse response = httpclient.execute(httpGet);
				
				HttpEntity entity = response.getEntity();
				StringWriter writer = new StringWriter();
				IOUtils.copy(entity.getContent(), writer);
				
				System.out.println(writer.toString());
				response.close();
			} catch (URISyntaxException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (ClientProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			end = true;
		}
		

	}
}
