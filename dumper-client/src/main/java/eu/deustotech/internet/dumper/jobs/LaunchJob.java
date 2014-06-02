package eu.deustotech.internet.dumper.jobs;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class LaunchJob implements Job {

	public static String TASK_ID = "TASK_ID";
	public static String SESSION = "SESSION";

	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {		
		
	}
}
