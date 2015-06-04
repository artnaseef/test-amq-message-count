package org.apache.activemq.bugs;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by art on 6/4/15.
 */
public class TestBrokerMessageCountAtStartup {

  private static File dataDirectory;
  private static File kahaDbDirectory;

  @BeforeClass
  public static void configure() {
    String dataParentDir = System.getProperty("org.apache.activemq.bugs.data_parent_dir");
    assertNotNull(dataParentDir);

    dataDirectory = new File(dataParentDir, "testBrokerMessageCountAtStartup");
    kahaDbDirectory = new File(dataDirectory, "kahadb");
  }

  @Test(timeout = 60000)
  public void testStartupMessageCount() throws Exception {
    //
    // Start the broker once with an empty directory.
    //

    FileUtils.deleteDirectory(dataDirectory);

    BrokerService brokerService;

    brokerService = this.setupBrokerService();
//    BrokerService brokerService = new BrokerService();
//    brokerService.setDataDirectory(dataDirectory.getPath());
//    brokerService.setPersistent(true);
//    brokerService.setUseJmx(true);
//
//    KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
//    persistenceAdapter.setDirectory(kahaDbDirectory);
//    brokerService.setPersistenceAdapter(persistenceAdapter);

    brokerService.start();
    brokerService.waitUntilStarted();

    this.sendMessages(brokerService, "test.queue.01", 10);
    this.sendMessages(brokerService, "test.queue.02", 20);
    this.sendMessages(brokerService, "Consumer.groupX.VirtualTopic.aaa", 30);

    assertEquals(60, brokerService.getAdminView().getTotalMessageCount());

    brokerService.stop();
    brokerService.waitUntilStopped();


    //
    // PART 2 - reload the same broker (using a newly-created broker service) and validate the
    //          counts.
    //

    brokerService = this.setupBrokerService();

    brokerService.start();
    brokerService.waitUntilStarted();

    long count = brokerService.getAdminView().getTotalMessageCount();
    assertEquals(60, count);
  }

  protected BrokerService setupBrokerService() throws Exception {
    BrokerService brokerService = new BrokerService();
    brokerService.setDataDirectory(dataDirectory.getPath());
    brokerService.setPersistent(true);
    brokerService.setUseJmx(true);

    KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
    persistenceAdapter.setDirectory(kahaDbDirectory);
    brokerService.setPersistenceAdapter(persistenceAdapter);

    return brokerService;
  }

  protected void sendMessages(BrokerService brokerService, String queueName, int count)
      throws Exception {

    ActiveMQConnection connection = ActiveMQConnection
        .makeConnection(brokerService.getVmConnectorURI().toString());

    try {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(new ActiveMQQueue(queueName));

      int cur;
      cur = 0;

      while (cur < count) {
        TextMessage textMessage = session.createTextMessage(String.format("message #%05d", cur));
        producer.send(textMessage);
        cur++;
      }
    } finally {
      connection.close();
    }
  }
}
