NB! If on mac, remove or comment the following line: 
The line containing ".config("spark.sql.warehouse.dir", "file:///C:/temp")"

NB! ".master("local[*]")" is to utilise all cores when running the program locally. Should not be included on a cluster

Run program
sbt "run pathToGraphFile"