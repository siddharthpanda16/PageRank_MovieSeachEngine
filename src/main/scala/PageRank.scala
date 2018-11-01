import org.apache.spark.{SparkConf, SparkContext}

  object PageRank {

    def main(args: Array[String]) {
      if (args.length < 2) {
        System.err.println("Enter three input parameters -> <Flights data csv> <iterations> <output location>")
        System.exit(1)
      }

      val sc = new SparkContext(new SparkConf().setAppName("Page Rank")) //setMaster("local")->uncomment if locally run

      val flightsData = sc.textFile(args(0)) // input flights data
      val numberOfIteration = if (args.length > 1) args(1).toInt else 15 //set no of iter from I/P else set 15 as default
      val count = flightsData.count() // count number of links

      val routes = flightsData.map { r =>
        val airport = r.split(",")        // filter out origin airport code and destination airport code
        (airport(1), airport(4))
      }

      val flightHeader = routes.first           //column headers
      val filteredRoutes = routes.filter(line => line != flightHeader)  //filter out column headers

      val routeLinks = filteredRoutes.groupByKey().cache()        // group number of the in-links for each airport

      var airportRanks = routeLinks.mapValues(v => 10.0)          // initialize the rank of airports to 10

      for (i <- 1 to numberOfIteration) {
        val sharedRankValues = routeLinks.join(airportRanks)
          .values
          .flatMap { case (airportCodes, rank) =>
            val size = airportCodes.size
            airportCodes.map(airportCode => (airportCode, rank / size))
          }
        airportRanks = sharedRankValues.reduceByKey(_ + _).mapValues(0.15/count + 0.85 * _)
      }

      val finalOutput = airportRanks.sortBy(-_._2).collect()
      sc.parallelize(finalOutput).saveAsTextFile(args(2))
    }
  }
