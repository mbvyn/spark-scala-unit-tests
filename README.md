# Spark Unit Tests
This project was created to show how to create unit tests for spark jobs
whether it batch or stream processing.

## Project overview
### Components

Application  |  Version
--|--
Scala | 2.13.7
Spark | 3.2.1
Scalatest | 3.2.12

### Description of the project
#### Spark jobs
There is two spark job for batch processing with a [straightforward](/src/main/scala/org/unit/batch/Straightforward.scala) 
and [modular](/src/main/scala/org/unit/batch/Modular.scala) style
and one for [stream processing](/src/main/scala/org/unit/stream/ModularStreaming.scala).\
If you would like to execute these jobs be sure you unzip a file in the [resource directory](/src/main/resources).

#### Unit Tests
For each spark job was created separate unit tests:
* [StraightforwardTest](/src/test/scala/org/unit/batch/StraightforwardTest.scala)
* [ModularTest](/src/test/scala/org/unit/batch/ModularTest.scala)
* [ModularStreamingTest](/src/test/scala/org/unit/stream/ModularStreamingTest.scala)

All tests share the same spark session provided via [SparkSessionTestWrapper](/src/test/scala/org/unit/SparkSessionTestWrapper.scala).\
Expected data and additional methods for batch and stream processing are stored in
[BatchTest](/src/test/scala/org/unit/batch/BatchTest.scala) and [StreamTest](/src/test/scala/org/unit/stream/StreamTest.scala) respectively.

#### Class relationship diagram
<p align="center"><img src="/UnitTests.png"></p>
