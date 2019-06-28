package org.apache.nifi.controller.status.history;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.apache.nifi.util.RingBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.*;

public class VolatileComponentStatusRepositoryTest {

  private static final Logger logger =
      LoggerFactory.getLogger(VolatileComponentStatusRepositoryTest.class);

  private VolatileComponentStatusRepository repo = new VolatileComponentStatusRepository();

  private static RingBuffer<Date> timestamps1;
  private static RingBuffer<Date> timestamps2;

  // 1_561_680_000 -> 6/28 2019
  // Integer.MAX_VALUE = 2_147_483_647


  @BeforeClass
  public static void createBuffers() {
    int bufSize1 = Integer.MAX_VALUE;
    int bufSize2 = 288;

    timestamps1 = new RingBuffer<>(bufSize1);
    for (int i = 0; i < bufSize1; i++) {
      timestamps1.add(new Date(i*1000));
    }
    assertEquals(bufSize1, timestamps1.getSize());

    timestamps2 = new RingBuffer<>(bufSize2);
    for (int i = 0; i < bufSize2; i++) {
      timestamps2.add(new Date(i*1000));
    }
    assertEquals(bufSize2, timestamps2.getSize());

    logger.info(">>>> First: " + timestamps1.getOldestElement());
    logger.info(">>>> Last:  " + timestamps1.getNewestElement());

    logger.info(">>>> maxint: " + Integer.MAX_VALUE);
    logger.info(">>>> Now: " + new Date(Integer.MAX_VALUE));
  }


  private Date asDate(LocalDateTime localDateTime) {
    return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
  }

  @Test
  public void testFilterDatesReturnAll() {
    logger.info("\n>>>> TEST testFilterDatesReturnAll");
    List<Date> dates = repo.filterDates(timestamps1, null, null, Integer.MAX_VALUE);
    assertEquals(timestamps1.getSize(), dates.size());
    assertTrue(dates.equals(timestamps1.asList()));
  }

  @Test
  public void testFilterDatesUsingPreferredDataPoints() {
    logger.info("\n>>>> TEST testFilterDatesReturnLastEntry");
    List<Date> dates = repo.filterDates(timestamps2, null, null, 1);
    assertEquals(1, dates.size());
    assertEquals(dates.get(0), timestamps2.getNewestElement());

    dates = repo.filterDates(timestamps2, null, null, 15);
    assertEquals(15, dates.size());
    assertEquals(dates.get(dates.size()-1), timestamps2.getNewestElement());
  }

  @Test
  public void testFilterDatesUsingStartFilter() {
    logger.info("\n>>>> TEST testFilterDatesUsingStartFilter");

    LocalDateTime localStart = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
    Date start = asDate(localStart);

    logger.info(">>>> Start: " + start.toString());
    List<Date> dates = repo.filterDates(timestamps1, start, null, Integer.MAX_VALUE);
    assertEquals(start, dates.get(0));
    assertEquals(timestamps1.getNewestElement(), dates.get(dates.size()-1));
  }

//  @Test
//  public void testFilterDatesUsingEndFilter() {
//    logger.info("\n>>>> TEST testFilterDatesUsingEndFilter");
//    assertTrue(timestamps1.getSize() == bufSize1);
//
//    LocalDateTime localStart = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
//    Date start = asDate(localStart);
//    LocalDateTime localEnd = LocalDateTime.of(1970, 2, 1,1, 0, 0, 0);
//    Date end = asDate(localEnd);
//
//    logger.info(">>>> Start: " + start.toString());
//    List<Date> dates = repo.filterDates(timestamps1, start, null, Integer.MAX_VALUE);
//    assertEquals(start, dates.get(0));
//    assertEquals(buffer1.get(bufSize1 -1), dates.get(dates.size()-1));
//
//
//    dates = repo.filterDates(timestamps1, null, end, Integer.MAX_VALUE);
//    assertEquals(buffer1.get(0), dates.get(0));
//    assertEquals(localEnd, dates.get(dates.size()-1));
//
//    dates = repo.filterDates(timestamps1, start, end, Integer.MAX_VALUE);
//    assertEquals(asDate(LocalDateTime.of(1970, 1, 1, 0, 0, 0)), dates.get(0));
//    assertEquals(asDate(LocalDateTime.of(1970, 2, 1, 0, 0, 0)), dates.get(dates.size()-1));
//
//    dates = repo.filterDates(timestamps1, start, end, 2);
//    assertEquals(2, dates.size());
//    assertEquals(asDate(LocalDateTime.of(1970, 1, 31, 59, 59, 59)), dates.get(0));
//    assertEquals(asDate(LocalDateTime.of(1970, 2, 1, 0, 0, 0)), dates.get(dates.size()-1));
//  }
//
//  @Test
//  public void testFilterDatesUsingStartAndEndFilter() {
//    logger.info("\n>>>> TEST testFilterDatesUsingStartAndEndFilter");
//    assertTrue(timestamps1.getSize() == bufSize1);
//
//    LocalDateTime localStart = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
//    Date start = asDate(localStart);
//    LocalDateTime localEnd = LocalDateTime.of(1970, 2, 1,1, 0, 0, 0);
//    Date end = asDate(localEnd);
//
//    logger.info(">>>> Start: " + start.toString());
//    List<Date> dates = repo.filterDates(timestamps1, start, null, Integer.MAX_VALUE);
//    assertEquals(start, dates.get(0));
//    assertEquals(buffer1.get(bufSize1 -1), dates.get(dates.size()-1));
//
//
//    dates = repo.filterDates(timestamps1, null, end, Integer.MAX_VALUE);
//    assertEquals(buffer1.get(0), dates.get(0));
//    assertEquals(localEnd, dates.get(dates.size()-1));
//
//    dates = repo.filterDates(timestamps1, start, end, Integer.MAX_VALUE);
//    assertEquals(asDate(LocalDateTime.of(1970, 1, 1, 0, 0, 0)), dates.get(0));
//    assertEquals(asDate(LocalDateTime.of(1970, 2, 1, 0, 0, 0)), dates.get(dates.size()-1));
//
//    dates = repo.filterDates(timestamps1, start, end, 2);
//    assertEquals(2, dates.size());
//    assertEquals(asDate(LocalDateTime.of(1970, 1, 31, 59, 59, 59)), dates.get(0));
//    assertEquals(asDate(LocalDateTime.of(1970, 2, 1, 0, 0, 0)), dates.get(dates.size()-1));
//  }
//
//  @Test
//  public void testFilterDatesUsingStartEndAndPreferredFilter() {
//    logger.info("\n>>>> TEST testFilterDatesUsingStartEndAndPreferredFilter");
//    assertTrue(timestamps1.getSize() == bufSize1);
//
//    LocalDateTime localStart = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
//    Date start = asDate(localStart);
//    LocalDateTime localEnd = LocalDateTime.of(1970, 2, 1,1, 0, 0, 0);
//    Date end = asDate(localEnd);
//
//    logger.info(">>>> Start: " + start.toString());
//    List<Date> dates = repo.filterDates(timestamps1, start, null, Integer.MAX_VALUE);
//    assertEquals(start, dates.get(0));
//    assertEquals(buffer1.get(bufSize1 -1), dates.get(dates.size()-1));
//
//
//    dates = repo.filterDates(timestamps1, null, end, Integer.MAX_VALUE);
//    assertEquals(buffer1.get(0), dates.get(0));
//    assertEquals(localEnd, dates.get(dates.size()-1));
//
//    dates = repo.filterDates(timestamps1, start, end, Integer.MAX_VALUE);
//    assertEquals(asDate(LocalDateTime.of(1970, 1, 1, 0, 0, 0)), dates.get(0));
//    assertEquals(asDate(LocalDateTime.of(1970, 2, 1, 0, 0, 0)), dates.get(dates.size()-1));
//
//    dates = repo.filterDates(timestamps1, start, end, 2);
//    assertEquals(2, dates.size());
//    assertEquals(asDate(LocalDateTime.of(1970, 1, 31, 59, 59, 59)), dates.get(0));
//    assertEquals(asDate(LocalDateTime.of(1970, 2, 1, 0, 0, 0)), dates.get(dates.size()-1));
//  }

}
