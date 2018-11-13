package com.oreilly.learningsparkexamples.java;

import com.oreilly.learningsparkexamples.java.CallLog;
import com.oreilly.learningsparkexamples.java.ChapterSixExample.SumInts;
import com.oreilly.learningsparkexamples.java.ChapterSixExample.VerifyCallLogs;
import com.oreilly.learningsparkexamples.java.ChapterSixExample;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ChapterSixExampleTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void callInput0Output0() {

    // Arrange
    final VerifyCallLogs objectUnderTest = new VerifyCallLogs();
    final CallLog[] input = {};

    // Act
    final CallLog[] retval = objectUnderTest.call(input);

    // Assert result
    Assert.assertArrayEquals(new CallLog[] {}, retval);
  }

  @Test
  public void callInput1Output0() {

    // Arrange
    final VerifyCallLogs objectUnderTest = new VerifyCallLogs();
    final CallLog callLog = new CallLog();
    callLog.mylong = 0.0;
    callLog.callsign = "";
    callLog.mylat = null;
    callLog.contactlat = 0.0;
    callLog.contactlong = 0.0;
    final CallLog[] input = {callLog};

    // Act
    final CallLog[] retval = objectUnderTest.call(input);

    // Assert result
    Assert.assertArrayEquals(new CallLog[] {}, retval);
  }

  @Test
  public void callInput1Output1() {

    // Arrange
    final VerifyCallLogs objectUnderTest = new VerifyCallLogs();
    final CallLog callLog = new CallLog();
    callLog.mylong = 0.0;
    callLog.callsign = "";
    callLog.mylat = 0.0;
    callLog.contactlat = 0.0;
    callLog.contactlong = 0.0;
    final CallLog[] input = {callLog};

    // Act
    final CallLog[] retval = objectUnderTest.call(input);

    // Assert result
    Assert.assertNotNull(retval);
    Assert.assertEquals(1, retval.length);
    Assert.assertNotNull(retval[0]);
    Assert.assertEquals(new Double(0.0), retval[0].mylong);
    Assert.assertEquals("", retval[0].callsign);
    Assert.assertEquals(new Double(0.0), retval[0].mylat);
    Assert.assertEquals(new Double(0.0), retval[0].contactlat);
    Assert.assertEquals(new Double(0.0), retval[0].contactlong);
  }

  @Test
  public void callInput1Output02() {

    // Arrange
    final VerifyCallLogs objectUnderTest = new VerifyCallLogs();
    final CallLog callLog = new CallLog();
    callLog.mylong = 0.0;
    callLog.callsign = "";
    callLog.mylat = 0.0;
    callLog.contactlat = null;
    callLog.contactlong = 0.0;
    final CallLog[] input = {callLog};

    // Act
    final CallLog[] retval = objectUnderTest.call(input);

    // Assert result
    Assert.assertArrayEquals(new CallLog[] {}, retval);
  }

  @Test
  public void callInput1Output03() {

    // Arrange
    final VerifyCallLogs objectUnderTest = new VerifyCallLogs();
    final CallLog[] input = {null};

    // Act
    final CallLog[] retval = objectUnderTest.call(input);

    // Assert result
    Assert.assertArrayEquals(new CallLog[] {}, retval);
  }

  @Test
  public void callInputNullOutput0() {

    // Arrange
    final VerifyCallLogs objectUnderTest = new VerifyCallLogs();
    final CallLog[] input = null;

    // Act
    final CallLog[] retval = objectUnderTest.call(input);

    // Assert result
    Assert.assertArrayEquals(new CallLog[] {}, retval);
  }

  @Test
  public void mainInput0OutputException() throws Exception {

    // Arrange
    final String[] args = {};

    // Act
    thrown.expect(Exception.class);
    ChapterSixExample.main(args);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void callInputZeroZeroOutputZero2() {
    // Arrange
    final SumInts objectUnderTest = new SumInts();
    final Integer x = 0;
    final Integer y = 0;
    // Act
    final Integer retval = objectUnderTest.call(x, y);
    // Assert result
    Assert.assertEquals(new Integer(0), retval);
  }
}
