#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.feature import HashingTF


if __name__ == "__main__":
    sc = SparkContext(appName="PythonBookExample")

    # Load 2 types of emails from text files: spam and ham (non-spam).
    # Each line has text from one email.
    spam = sc.textFile("files/spam.txt")
    ham = sc.textFile("files/ham.txt")

    # Create a HashingTF instance to map email text to vectors of 100 features.
    tf = HashingTF(numFeatures = 100)
    # Each email is split into words, and each word is mapped to one feature.
    spamFeatures = spam.map(lambda email: tf.transform(email.split(" ")))
    hamFeatures = ham.map(lambda email: tf.transform(email.split(" ")))

    # Create LabeledPoint datasets for positive (spam) and negative (ham) examples.
    positiveExamples = spamFeatures.map(lambda features: LabeledPoint(1, features))
    negativeExamples = hamFeatures.map(lambda features: LabeledPoint(0, features))
    training_data = positiveExamples.union(negativeExamples)
    training_data.cache() # Cache data since Logistic Regression is an iterative algorithm.

    # Run Logistic Regression using the SGD optimizer.
    # regParam is model regularization, which can make models more robust.
    model = LogisticRegressionWithSGD.train(training_data)

    # Test on a positive example (spam) and a negative one (ham).
    # First apply the same HashingTF feature transformation used on the training data.
    posTestExample = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    negTestExample = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))

    # Now use the learned model to predict spam/ham for new emails.
    print "Prediction for positive test example: %g" % model.predict(posTestExample)
    print "Prediction for negative test example: %g" % model.predict(negTestExample)

    sc.stop()
