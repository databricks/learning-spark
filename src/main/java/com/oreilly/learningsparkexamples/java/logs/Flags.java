package com.oreilly.learningsparkexamples.java.logs;

import org.apache.commons.cli.*;
import org.apache.spark.streaming.Duration;

public class Flags {
  private static Flags THE_INSTANCE = new Flags();

  private Duration windowLength;
  private Duration slideInterval;
  private String logsDirectory;
  private String outputHtmlFile;
  private String checkpointDirectory;
  private String indexHtmlTemplate;
  private String outputDirectory;

  private boolean initialized = false;

  private Flags() {}

  public Duration getWindowLength() {
    return windowLength;
  }

  public Duration getSlideInterval() {
    return slideInterval;
  }

  public String getLogsDirectory() {
    return logsDirectory;
  }

  public String getOutputHtmlFile() {
    return outputHtmlFile;
  }

  public String getCheckpointDirectory() {
    return checkpointDirectory;
  }

  public String getOutputDirectory() {
    return outputDirectory;
  }

  public String getIndexHtmlTemplate() {
    return indexHtmlTemplate;
  }

  public static Flags getInstance() {
    if (!THE_INSTANCE.initialized) {
      throw new RuntimeException("Flags have not been initalized");
    }
    return THE_INSTANCE;
  }

  public static void setFromCommandLineArgs(Options options, String[] args) {
    CommandLineParser parser = new PosixParser();
    try {
      CommandLine cl = parser.parse(options, args);
      THE_INSTANCE.windowLength = new Duration(Integer.parseInt(
          cl.getOptionValue(LogAnalyzerAppMain.WINDOW_LENGTH, "30")) * 1000);
      THE_INSTANCE.slideInterval = new Duration(Integer.parseInt(
          cl.getOptionValue(LogAnalyzerAppMain.SLIDE_INTERVAL, "5")) * 1000);
      THE_INSTANCE.logsDirectory = cl.getOptionValue(
          LogAnalyzerAppMain.LOGS_DIRECTORY, "/tmp/logs");
      THE_INSTANCE.outputHtmlFile = cl.getOptionValue(
          LogAnalyzerAppMain.OUTPUT_HTML_FILE, "/tmp/log_stats.html");
      THE_INSTANCE.checkpointDirectory = cl.getOptionValue(
          LogAnalyzerAppMain.CHECKPOINT_DIRECTORY, "/tmp/log-analyzer-streaming");
      THE_INSTANCE.indexHtmlTemplate = cl.getOptionValue(
          LogAnalyzerAppMain.INDEX_HTML_TEMPLATE,
          "./src/main/resources/index.html.template");
      THE_INSTANCE.outputDirectory = cl.getOptionValue(
        LogAnalyzerAppMain.OUTPUT_DIRECTORY, "/tmp/pandaout");
      THE_INSTANCE.initialized = true;
    } catch (ParseException e) {
      THE_INSTANCE.initialized = false;
      System.err.println("Parsing failed.  Reason: " + e.getMessage());
    }
  }
}
