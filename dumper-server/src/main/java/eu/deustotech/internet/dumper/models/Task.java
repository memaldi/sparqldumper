package eu.deustotech.internet.dumper.models;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.GenericGenerator;


@Entity
@Table(name="Tasks")
public class Task {
	
	public static String RUNNING = "RUNNING";
	public static String PAUSED = "PAUSED";
	public static String DONE = "DONE";
	
    private Long id;

    private String endpoint;
    private String graph;
    private Date startTime;
    private Date endTime;
    private Date paused_since;
    private Long offset;
    private String status;

    public Task() {
    }

    @Id
    @GeneratedValue(generator="increment")
    @GenericGenerator(name="increment", strategy = "increment")
    public Long getId() {
        return id;
    }

    private void setId(Long id) {
        this.id = id;
    }

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public String getGraph() {
		return graph;
	}

	public void setGraph(String graph) {
		this.graph = graph;
	}

	@Temporal(TemporalType.TIMESTAMP)
	public Date getStart_time() {
		return startTime;
	}

	public void setStart_time(Date start_time) {
		this.startTime = start_time;
	}

	@Temporal(TemporalType.TIMESTAMP)
	public Date getEnd_time() {
		return endTime;
	}

	public void setEnd_time(Date end_time) {
		this.endTime = end_time;
	}
	
	@Temporal(TemporalType.TIMESTAMP)
	public Date getPaused_since() {
		return paused_since;
	}

	public void setPaused_since(Date paused_since) {
		this.paused_since = paused_since;
	}

	public Long getOffset() {
		return offset;
	}

	public void setOffset(Long offset) {
		this.offset = offset;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
    
}