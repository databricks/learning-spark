package com.oreilly.learningsparkexamples.java;
import java.io.Serializable;


class HappyPerson implements Serializable {
  private String name;
  private String favouriteBeverage;
  public HappyPerson() {}
  public HappyPerson(String n, String b) {
    name = n; favouriteBeverage = b;
  }
  public String getName() { return name; }
  public void setName(String n) { name = n; }
  public String getFavouriteBeverage() { return favouriteBeverage; }
  public void setFavouriteBeverage(String b) { favouriteBeverage = b; }
};
