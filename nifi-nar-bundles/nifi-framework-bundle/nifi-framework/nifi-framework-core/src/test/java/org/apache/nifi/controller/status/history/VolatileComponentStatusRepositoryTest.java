package org.apache.nifi.controller.status.history;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.nifi.util.RingBuffer;

import static org.junit.Assert.*;

/**
 * This class verifies the VolatileComponentStatusRepository getConnectionStatusHistory method
 * honors the start/end/preferredDataPoints variables.
 */
public class VolatileComponentStatusRepositoryTest {

  private final VolatileComponentStatusRepository repo = new VolatileComponentStatusRepository();
  private static RingBuffer<Date> timestamps;
  private static final int FIVE_MINUTES = 300000;

  @SuppressWarnings("SpellCheckingInspection") @BeforeClass
  public static void createBuffers() {
    int BUFSIZE = 2_500_000;
    timestamps = new RingBuffer<>(BUFSIZE);
    // create a buffer containing date objects at five-minute intervals
    // This provides dates up to around Oct 1993
    for (long i = 0; i < BUFSIZE; i++) {
      timestamps.add(new Date(i * FIVE_MINUTES));
    }
    assertEquals(BUFSIZE, timestamps.getSize());
  }

  private static Date asDate(LocalDateTime localDateTime) {
    return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
  }

  @Test
  public void testFilterDatesReturnAll() {
    List<Date> dates = repo.filterDates(timestamps, null, null, Integer.MAX_VALUE);
    assertEquals(timestamps.getSize(), dates.size());
    assertTrue(dates.equals(timestamps.asList()));
  }

  @Test
  public void testFilterDatesUsingPreferredDataPoints() {
    List<Date> dates = repo.filterDates(timestamps, null, null, 1);
    assertEquals(1, dates.size());
    assertEquals(timestamps.getNewestElement(), dates.get(0));

    int numPoints = 14;
    dates = repo.filterDates(timestamps, null, null, numPoints);
    assertEquals(numPoints, dates.size());
    assertEquals(timestamps.getNewestElement(), dates.get(dates.size()-1));
    assertEquals(timestamps.asList().get(timestamps.getSize() - numPoints), dates.get(0));
  }

  @Test
  public void testFilterDatesUsingStartFilter() {
    // Filter with date that exactly matches an entry in timestamps buffer
    Date start = asDate(LocalDateTime.of(1980, 1, 1, 0, 45, 0));
    List<Date> dates = repo.filterDates(timestamps, start, null, Integer.MAX_VALUE);
    assertEquals(start, dates.get(0));
    assertEquals(timestamps.getNewestElement(), dates.get(dates.size()-1));

    // filter using a date that does not exactly match the time, i.e., not on a five-minute mark
    start = asDate(LocalDateTime.of(1990, 1, 1, 3, 2, 0));
    dates = repo.filterDates(timestamps, start, null, Integer.MAX_VALUE);
    assertTrue(start.getTime() < dates.get(0).getTime());
    assertTrue(dates.get(0).getTime() < (start.getTime() + FIVE_MINUTES));
    assertEquals(timestamps.getNewestElement(), dates.get(dates.size()-1));
  }

  @Test
  public void testFilterDatesUsingEndFilter() {
    // Filter with date that exactly matches an entry in timestamps buffer
    Date end = asDate(LocalDateTime.of(1970, 2, 1,1, 10, 0));
    List<Date> dates = repo.filterDates(timestamps, null, end, Integer.MAX_VALUE);
    assertEquals(end, dates.get(dates.size()-1));
    assertEquals(timestamps.getOldestElement(), dates.get(0));

    // filter using a date that does not exactly match the times in buffer
    end = asDate(LocalDateTime.of(1970, 2, 1,1, 7, 0));
    dates = repo.filterDates(timestamps, null, end, Integer.MAX_VALUE);
    assertTrue(dates.get(dates.size()-1).getTime() < end.getTime());
    assertTrue((end.getTime() - FIVE_MINUTES) < dates.get(dates.size()-1).getTime());
    assertEquals(dates.get(0), timestamps.getOldestElement());
  }

  @Test
  public void testFilterDatesUsingStartAndEndFilter() {
    // Filter with dates that exactly matches entries in timestamps buffer
    Date start = asDate(LocalDateTime.of(1975, 3, 1, 3, 15, 0));
    Date end = asDate(LocalDateTime.of(1980, 4, 2,4, 25, 0));
    List<Date> dates = repo.filterDates(timestamps, start, end, Integer.MAX_VALUE);
    assertEquals(start, dates.get(0));
    assertEquals(end, dates.get(dates.size()-1));

    // Filter with dates that do not exactly matches entries in timestamps buffer
    start = asDate(LocalDateTime.of(1975, 3, 1, 3, 3, 0));
    end = asDate(LocalDateTime.of(1977, 4, 2,4, 8, 0));
    dates = repo.filterDates(timestamps, start, end, Integer.MAX_VALUE);
    assertTrue(start.getTime() < dates.get(0).getTime());
    assertTrue(dates.get(0).getTime() < (start.getTime() + FIVE_MINUTES));
    assertTrue(dates.get(dates.size()-1).getTime() < end.getTime());
    assertTrue((end.getTime() - FIVE_MINUTES) < dates.get(dates.size()-1).getTime());
  }

  @Test
  public void testFilterDatesUsingStartEndAndPreferredFilter() {
    // Filter with dates that exactly matches entries in timestamps buffer
    int numPoints = 5;
    Date start = asDate(LocalDateTime.of(1983, 1, 1, 0, 30, 0));
    Date end = asDate(LocalDateTime.of(1986, 2, 1,1, 0, 0));
    List<Date> dates = repo.filterDates(timestamps, start, end, numPoints);
    assertEquals(numPoints, dates.size());
    assertEquals(dates.get(dates.size()-1), end);
    assertEquals(dates.get(dates.size()-numPoints), new Date(end.getTime() - (numPoints-1)*FIVE_MINUTES));

    // Filter with dates that do not exactly matches entries in timestamps buffer
    start = asDate(LocalDateTime.of(1983, 1, 1, 0, 31, 0));
    end = asDate(LocalDateTime.of(1986, 2, 1,1, 59, 0));
    dates = repo.filterDates(timestamps, start, end, numPoints);
    assertTrue(dates.get(0).getTime() < new Date(end.getTime() - (numPoints-1)*FIVE_MINUTES).getTime());
    assertTrue(new Date(end.getTime() - (numPoints * FIVE_MINUTES)).getTime() < dates.get(0).getTime());
    assertTrue(dates.get(dates.size()-1).getTime() < end.getTime());
    assertTrue((end.getTime() - FIVE_MINUTES) < dates.get(dates.size()-1).getTime());
    assertEquals(numPoints, dates.size());
  }
}
