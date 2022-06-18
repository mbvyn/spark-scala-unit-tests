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
There is two spark job for batch processing with a [straightforward](Straightforward.scala) and [modular](Modular.scala) style
and one for [stream processing](ModularStreaming.scala).\
If you would like to execute these jobs be sure you unzip a file in the [resource directory](resources).

#### Unit Tests
For each spark job was created separate unit tests:
* [StraightforwardTest](StraightforwardTest.scala)
* [ModularTest](ModularTest.scala)
* [ModularStreamingTest](ModularStreamingTest.scala)

All tests share the same spark session provided via [SparkSessionTestWrapper](SparkSessionTestWrapper.scala).\
Expected data and additional methods for batch and stream processing are stored in
[BatchTest](BatchTest.scala) and [StreamTest](StreamTest.scala) respectively.

#### Class relationship diagram
<p align="center"><img src="/UnitTests.png"></p>
