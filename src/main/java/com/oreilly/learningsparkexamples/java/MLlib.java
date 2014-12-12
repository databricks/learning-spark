/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oreilly.learningsparkexamples.java;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

public final class MLlib {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("JavaBookExample");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    // Load 2 types of emails from text files: spam and ham (non-spam).
    // Each line has text from one email.
    JavaRDD<String> spam = sc.textFile("files/spam.txt");
    JavaRDD<String> ham = sc.textFile("files/ham.txt");

    // Create a HashingTF instance to map email text to vectors of 100 features.
    final HashingTF tf = new HashingTF(100);

    // Each email is split into words, and each word is mapped to one feature.
    // Create LabeledPoint datasets for positive (spam) and negative (ham) examples.
    JavaRDD<LabeledPoint> positiveExamples = spam.map(new Function<String, LabeledPoint>() {
      @Override public LabeledPoint call(String email) {
        return new LabeledPoint(1, tf.transform(Arrays.asList(email.split(" "))));
      }
    });
    JavaRDD<LabeledPoint> negativeExamples = ham.map(new Function<String, LabeledPoint>() {
      @Override public LabeledPoint call(String email) {
        return new LabeledPoint(0, tf.transform(Arrays.asList(email.split(" "))));
      }
    });
    JavaRDD<LabeledPoint> trainingData = positiveExamples.union(negativeExamples);
    trainingData.cache(); // Cache data since Logistic Regression is an iterative algorithm.

    // Create a Logistic Regression learner which uses the LBFGS optimizer.
    LogisticRegressionWithSGD lrLearner = new LogisticRegressionWithSGD();
    // Run the actual learning algorithm on the training data.
    LogisticRegressionModel model = lrLearner.run(trainingData.rdd());

    // Test on a positive example (spam) and a negative one (ham).
    // First apply the same HashingTF feature transformation used on the training data.
    Vector posTestExample =
        tf.transform(Arrays.asList("O M G GET cheap stuff by sending money to ...".split(" ")));
    Vector negTestExample =
        tf.transform(Arrays.asList("Hi Dad, I started studying Spark the other ...".split(" ")));
    // Now use the learned model to predict spam/ham for new emails.
    System.out.println("Prediction for positive test example: " + model.predict(posTestExample));
    System.out.println("Prediction for negative test example: " + model.predict(negTestExample));

    sc.stop();
  }
}
