package org.apache.nifi.controller.status.history;

import java.util.Date;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class VolatileComponentStatusRepositoryTest {

  private static final Logger logger =
      LoggerFactory.getLogger(VolatileComponentStatusRepositoryTest.class);

  @Test public void testGetConnectionStatusHistory() {

    ComponentDetails componentDetails = new ComponentDetails("id1", "groupId", "componentName",
        "componentType", "sourceName", "destinationName", "remoteUri");

    VolatileComponentStatusRepository repo = new VolatileComponentStatusRepository();

    ComponentStatusHistory componentStatusHistory = new ComponentStatusHistory(componentDetails,
        10);

//    repo.componentStatusHistories.put("history01", componentStatusHistory);
//    repo.timestamps.add(new Date());
//
//    StatusHistory statusHistory = repo.getConnectionStatusHistory("history01", null, null, 10);
//    logger.info("dateGeneratoed: " + statusHistory.getDateGenerated());
//    logger.info("statusMetric: " + statusHistory.getStatusSnapshots().get(0).toString());
  }

}