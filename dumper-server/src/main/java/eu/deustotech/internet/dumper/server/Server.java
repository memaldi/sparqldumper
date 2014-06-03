package eu.deustotech.internet.dumper.server;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {
	
	public static void main(String args[]) {
		try {
			
			Logger log = LoggerFactory.getLogger(Server.class);
			SchedulerFactory sf = new StdSchedulerFactory();
		    Scheduler sched = sf.getScheduler();

		    log.info("------- Initialization Complete -----------");

		    log.info("------- (Not Scheduling any Jobs - relying on a remote client to schedule jobs --");

		    log.info("------- Starting Scheduler ----------------");
		    sched.start();
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
