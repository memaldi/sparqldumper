package eu.deustotech.internet.dumper.jobmanager;

public class JobManagerFactory {

	private JobManager jobManager;
	
	public JobManagerFactory(){
		
	}
	
	public JobManager getJobManager() {
		if (this.jobManager == null) {
			this.jobManager = new JobManager();
		}
		return this.jobManager;
	}
	
	public void close() {
		if (this.jobManager != null) {
			this.jobManager.close();
			this.jobManager = null;
		}
	}
}
