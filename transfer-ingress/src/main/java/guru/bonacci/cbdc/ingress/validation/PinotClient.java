package guru.bonacci.cbdc.ingress.validation;

import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class PinotClient {

	private transient Connection pinotConnection;

  @PostConstruct
  void init() {
    pinotConnection = ConnectionFactory.fromHostList("localhost:8099");
  }

  @PreDestroy
  void close() {
  	pinotConnection.close();
  }
  
  public Connection getConnection() {
  	return this.pinotConnection;
  }
}
