package com.oreilly.learningsparkexamples.java;

import java.io.Serializable;

public class CallLog implements Serializable {
  private String callsign;
  private Double contactlat;
  private Double contactlong;
  private Double mylat;
  private Double mylong;

  public String getCallsign() {
    return callsign;
  }

  public void setCallsign(String callsign) {
    this.callsign = callsign;
  }

  public Double getContactlat() {
    return contactlat;
  }

  public void setContactlat(Double contactlat) {
    this.contactlat = contactlat;
  }

  public Double getContactlong() {
    return contactlong;
  }

  public void setContactlong(Double contactlong) {
    this.contactlong = contactlong;
  }

  public Double getMylat() {
    return mylat;
  }

  public void setMylat(Double mylat) {
    this.mylat = mylat;
  }

  public Double getMylong() {
    return mylong;
  }

  public void setMylong(Double mylong) {
    this.mylong = mylong;
  }
}
