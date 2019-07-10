package org.apache.nifi.controller.status.history;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nifi.util.NiFiProperties;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class verifies the VolatileComponentStatusRepository getConnectionStatusHistory method
 * honors the start/end/preferredDataPoints variables by testing the filterDates function.
 */
public class VolatileComponentStatusRepositoryTest {

  private static VolatileComponentStatusRepository repo1;
  private static VolatileComponentStatusRepository repo2;
  private static VolatileComponentStatusRepository repo3;
  private static final int FIVE_MINUTES = 300000;
  private static int BUFSIZE1 = 1_000_000;
  private static int BUFSIZE2 = 1000;;
  private static int BUFSIZE3 = 10;

  private static final Logger logger =
      LoggerFactory.getLogger(VolatileComponentStatusRepositoryTest.class);

  @BeforeClass
  public static void createBuffers() {
    NiFiProperties props1 = mock(NiFiProperties.class);
    when(props1.getIntegerProperty(anyString(), anyInt())).thenReturn(BUFSIZE1);
    repo1 = new VolatileComponentStatusRepository(props1);
    // Fill the repo1 buffer completely with Date objects at five-minute intervals
    // This provides dates up to around Jul 1979
    for (long i = 0; i < BUFSIZE1; i++) {
      repo1.timestamps.add(new Date(i * FIVE_MINUTES));
    }
    assertEquals(BUFSIZE1, repo1.timestamps.getSize());

    NiFiProperties props2 = mock(NiFiProperties.class);
    when(props2.getIntegerProperty(anyString(), anyInt())).thenReturn(BUFSIZE2);
    repo2 = new VolatileComponentStatusRepository(props2);
    int OFFSET = 10;
    // Verify partially filled buffers work as expected.
    for (long i = 0; i < BUFSIZE2 - OFFSET; i++) {
      repo2.timestamps.add(new Date(i * FIVE_MINUTES));
    }
    assertEquals(BUFSIZE2 - OFFSET, repo2.timestamps.getSize());

    int BUFSIZE3 = 10;
    NiFiProperties props3 = mock(NiFiProperties.class);
    when(props3.getIntegerProperty(anyString(), anyInt())).thenReturn(BUFSIZE3);
    repo3 = new VolatileComponentStatusRepository(props3);
  }

  private static void pause(int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private Date asDate(LocalDateTime localDateTime) {
    return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
  }

  @Test
  public void testFilterDatesReturnAll() {
    logger.info(">>>> testFilterDatesReturnAll...");
    List<Date> dates = repo1.filterDates(null, null, Integer.MAX_VALUE);
    assertEquals(repo1.timestamps.getSize(), dates.size());
    assertTrue(dates.equals(repo1.timestamps.asList()));
    repo1.timestamps.add(new Date());
  }

  @Test
  public void testFilterDatesUsingPreferredDataPoints() {
    logger.info(">>>> testFilterDatesUsingPreferredDataPoints...");
    List<Date> dates = repo1.filterDates(null, null, 1);
    assertEquals(1, dates.size());
    assertEquals(repo1.timestamps.getNewestElement(), dates.get(0));

    int numPoints = 14;
    dates = repo1.filterDates(null, null, numPoints);
    assertEquals(numPoints, dates.size());
    assertEquals(repo1.timestamps.getNewestElement(), dates.get(dates.size()-1));
    assertEquals(repo1.timestamps.asList().get(repo1.timestamps.getSize() - numPoints), dates.get(0));
  }

  @Test
  public void testFilterDatesUsingStartFilter() {
    logger.info(">>>> testFilterDatesUsingStartFilter...");
    // Filter with date that exactly matches an entry in timestamps buffer
    Date start = asDate(LocalDateTime.of(1978, 1, 1, 0, 45, 0));
    List<Date> dates = repo1.filterDates(start, null, Integer.MAX_VALUE);
    assertEquals(start, dates.get(0));
    assertEquals(repo1.timestamps.getNewestElement(), dates.get(dates.size()-1));

    // filter using a date that does not exactly match the time, i.e., not on a five-minute mark
    start = asDate(LocalDateTime.of(1974, 1, 1, 3, 2, 0));
    dates = repo1.filterDates(start, null, Integer.MAX_VALUE);
    assertTrue(start.getTime() < dates.get(0).getTime());
    assertTrue(dates.get(0).getTime() < (start.getTime() + FIVE_MINUTES));
    assertEquals(repo1.timestamps.getNewestElement(), dates.get(dates.size()-1));
  }

  @Test
  public void testFilterDatesUsingEndFilter() {
    logger.info(">>>> testFilterDatesUsingEndFilter...");
    // Filter with date that exactly matches an entry in timestamps buffer
    Date end = asDate(LocalDateTime.of(1970, 2, 1,1, 10, 0));
    List<Date> dates = repo1.filterDates(null, end, Integer.MAX_VALUE);
    assertEquals(end, dates.get(dates.size()-1));
    assertEquals(repo1.timestamps.getOldestElement(), dates.get(0));

    // filter using a date that does not exactly match the times in buffer
    end = asDate(LocalDateTime.of(1970, 2, 1,1, 7, 0));
    dates = repo1.filterDates(null, end, Integer.MAX_VALUE);
    assertTrue(dates.get(dates.size()-1).getTime() < end.getTime());
    assertTrue((end.getTime() - FIVE_MINUTES) < dates.get(dates.size()-1).getTime());
    assertEquals(dates.get(0), repo1.timestamps.getOldestElement());
  }

  @Test
  public void testFilterDatesUsingStartAndEndFilter() {
    logger.info(">>>> testFilterDatesUsingStartAndEndFilter...");
    // Filter with dates that exactly matches entries in timestamps buffer
    Date start = asDate(LocalDateTime.of(1975, 3, 1, 3, 15, 0));
    Date end = asDate(LocalDateTime.of(1978, 4, 2,4, 25, 0));
    List<Date> dates = repo1.filterDates(start, end, Integer.MAX_VALUE);
    assertEquals(start, dates.get(0));
    assertEquals(end, dates.get(dates.size()-1));

    // Filter with dates that do not exactly matches entries in timestamps buffer
    start = asDate(LocalDateTime.of(1975, 3, 1, 3, 3, 0));
    end = asDate(LocalDateTime.of(1977, 4, 2,4, 8, 0));
    dates = repo1.filterDates(start, end, Integer.MAX_VALUE);
    assertTrue(start.getTime() < dates.get(0).getTime());
    assertTrue(dates.get(0).getTime() < (start.getTime() + FIVE_MINUTES));
    assertTrue(dates.get(dates.size()-1).getTime() < end.getTime());
    assertTrue((end.getTime() - FIVE_MINUTES) < dates.get(dates.size()-1).getTime());
  }

  @Test
  public void testFilterDatesUsingStartEndAndPreferredFilter() {
    logger.info(">>>> testFilterDatesUsingStartEndAndPreferredFilter...");
    // Filter with dates that exactly matches entries in timestamps buffer
    int numPoints = 5;
    Date start = asDate(LocalDateTime.of(1977, 1, 1, 0, 30, 0));
    Date end = asDate(LocalDateTime.of(1977, 2, 1,1, 0, 0));
    List<Date> dates = repo1.filterDates(start, end, numPoints);
    assertEquals(numPoints, dates.size());
    assertEquals(dates.get(dates.size()-1), end);
    assertEquals(dates.get(dates.size()-numPoints), new Date(end.getTime() - (numPoints-1)*FIVE_MINUTES));

    // Filter with dates that do not exactly matches entries in timestamps buffer
    start = asDate(LocalDateTime.of(1975, 1, 1, 0, 31, 0));
    end = asDate(LocalDateTime.of(1978, 2, 1,1, 59, 0));
    dates = repo1.filterDates(start, end, numPoints);
    assertTrue(dates.get(0).getTime() < new Date(end.getTime() - (numPoints-1)*FIVE_MINUTES).getTime());
    assertTrue(new Date(end.getTime() - (numPoints * FIVE_MINUTES)).getTime() < dates.get(0).getTime());
    assertTrue(dates.get(dates.size()-1).getTime() < end.getTime());
    assertTrue((end.getTime() - FIVE_MINUTES) < dates.get(dates.size() - 1).getTime());
    assertEquals(numPoints, dates.size());
  }

  @Test
  public void testFilterWorksWithCircularBuffer() {
    logger.info(">>>> testFilterWorksWithCircularBuffer...");
    // Fill repo3 with Date objects at five-minute intervals
    // This repository is used to verify circular actions behave as expected.
    for (int i = 0; i < 25; i++) {
      repo3.timestamps.add(new Date(i * FIVE_MINUTES));
      List<Date> dates = repo3.filterDates(null, null, Integer.MAX_VALUE);
      if (i < BUFSIZE3 - 1) {
        assertEquals(null, repo3.timestamps.getOldestElement());
      } else {
        assertEquals(repo3.timestamps.getOldestElement(), dates.get(0));
      }
      assertEquals(repo3.timestamps.getNewestElement(), dates.get(dates.size() - 1));
    }
  }
}
