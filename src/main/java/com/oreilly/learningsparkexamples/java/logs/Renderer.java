package com.oreilly.learningsparkexamples.java.logs;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import scala.Tuple2;
import scala.Tuple4;

import java.io.*;
import java.util.List;
import java.util.Map;

public class Renderer implements Serializable {
  private String fileTemplate;

  public void render(LogStatistics allOfTime, LogStatistics lastWindow)
      throws Exception {
    if (fileTemplate == null) {
      fileTemplate = Files.toString(
          new File(Flags.getInstance().getIndexHtmlTemplate()),
          Charsets.UTF_8);
    }

    // TODO: Replace this hacky String replace with a proper HTML templating library.
    String output = fileTemplate;
    output = output.replace("${logLinesTable}", logLinesTable(allOfTime, lastWindow));
    output = output.replace("${contentSizesTable}", contentSizesTable(allOfTime, lastWindow));
    output = output.replace("${responseCodeTable}", responseCodeTable(allOfTime, lastWindow));
    output = output.replace("${topEndpointsTable}", topEndpointsTable(allOfTime, lastWindow));
    output = output.replace("${frequentIpAddressTable}", frequentIpAddressTable(allOfTime, lastWindow));

    Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
        Flags.getInstance().getOutputHtmlFile())));
    out.write(output);
    out.close();
  }

  public String logLinesTable(LogStatistics allOfTime, LogStatistics lastWindow) {
    return "<table class=\"table table-striped\">" +
        String.format("<tr><th>All Of Time:</th><td>%s</td></tr>",
            allOfTime.getContentSizeStats()._1()) +
        String.format("<tr><th>Last Time Window:</th><td>%s</td></tr>",
            lastWindow.getContentSizeStats()._1()) +
        "</table>";
  }

  public String contentSizesTable(LogStatistics allOfTime, LogStatistics lastWindow) {
    StringBuilder builder = new StringBuilder();
    builder.append("<table class=\"table table-striped\">");
    builder.append("<tr><th></th><th>All of Time</th><th> Last Time Window</th></tr>");
    Tuple4<Long, Long, Long, Long> totalStats = allOfTime.getContentSizeStats();
    Tuple4<Long, Long, Long, Long> lastStats = lastWindow.getContentSizeStats();
    builder.append(String.format("<tr><th>Avg:</th><td>%s</td><td>%s</td>",
        totalStats._1() > 0 ? totalStats._2() / totalStats._1() : "-",
        lastStats._1() > 0 ? lastStats._2() / lastStats._1() : "-"));
    builder.append(String.format("<tr><th>Min:</th><td>%s</td><td>%s</td>",
        totalStats._1() > 0 ? totalStats._3() : "-",
        lastStats._1() > 0 ? lastStats._3() : "-"));
    builder.append(String.format("<tr><th>Max:</th><td>%s</td><td>%s</td>",
        totalStats._1() > 0 ? totalStats._4() : "-",
        lastStats._1() > 0 ? lastStats._4() : "-"));
    builder.append("</table>");
    return builder.toString();
  }

  public String responseCodeTable(
      LogStatistics allOfTime, LogStatistics lastWindow) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("<table class=\"table table-striped\">");
    buffer.append("<tr><th>Response Code</th><th>All of Time</th><th> Last Time Window</th></tr>");
    Map<Integer, Long> lastWindowMap = lastWindow.getResponseCodeToCount();
    for(Map.Entry<Integer, Long> entry: allOfTime.getResponseCodeToCount().entrySet()) {
      buffer.append(String.format("<tr><td>%s</td><td>%s</td><td>%s</td>",
        entry.getKey(), entry.getValue(), lastWindowMap.get(entry.getKey())));
    }
    buffer.append("</table>");
    return buffer.toString();
  }

  public String frequentIpAddressTable(
      LogStatistics allOfTime, LogStatistics lastWindow) {
    StringBuilder builder = new StringBuilder();
    builder.append("<table class=\"table table-striped\">");
    builder.append("<tr><th>All of Time</th><th> Last Time Window</th></tr>");
    List<String> totalIpAddresses = allOfTime.getIpAddresses();
    List<String> windowIpAddresses = lastWindow.getIpAddresses();
    for (int i = 0; i < totalIpAddresses.size(); i++) {
      builder.append(String.format("<tr><td>%s</td><td>%s</td></tr>",
          totalIpAddresses.get(i),
          i < windowIpAddresses.size() ? windowIpAddresses.get(i) : "-"));
    }
    builder.append("</table>");
    return builder.toString();
  }

  public String topEndpointsTable(
      LogStatistics allOfTime, LogStatistics lastWindow) {
    StringBuilder builder = new StringBuilder();
    builder.append("<table class=\"table table-striped\">");
    builder.append("<tr><th>All of Time</th><th>Last Time Window</th></tr>");
    List<Tuple2<String, Long>> totalTopEndpoints = allOfTime.getTopEndpoints();
    List<Tuple2<String, Long>> windowTopEndpoints = lastWindow.getTopEndpoints();
    for (int i = 0; i < totalTopEndpoints.size(); i++) {
      builder.append(String.format("<tr><td>%s</td><td>%s</td></tr>",
          totalTopEndpoints.get(i)._1(),
          i < windowTopEndpoints.size() ? windowTopEndpoints.get(i)._1() : "-"));
    }
    builder.append("</table>");
    return builder.toString();
  }
}
