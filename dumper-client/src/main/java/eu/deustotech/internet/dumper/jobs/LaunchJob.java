package eu.deustotech.internet.dumper.jobs;

import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

public class LaunchJob implements InterruptableJob {

	public static String TASK_ID = "TASK_ID";
	public static String SESSION = "SESSION";
	
	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {		
		
	}

	public void interrupt() throws UnableToInterruptJobException {
	}
}
