package dk.dtu.mhoras.rdfsum

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import java.io._
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.commons.io.FilenameUtils
import org.apache.jena.rdf.model._
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.Lang
import scala.collection.mutable.ListBuffer

object RDFSummary {
  
  case class Triples(sub: String, pred: String, obj: String)
  case class TriplesTgt(sub: String, pred: String, obj: String, varT: String)
  case class TriplesVars(sub: String, varS: String, pred: String, obj: String, varT: String)
  
  // Get statements of a triple
  def getStmt(line:String) : Statement = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    // Create empty model
    val model: Model = ModelFactory.createDefaultModel()
    
    // Read the triple into the model
    RDFDataMgr.read(model, new ByteArrayInputStream(line.getBytes()), Lang.NTRIPLES)
    
    // List statements in the model
    val iter : StmtIterator  = model.listStatements()
    
    // Get first (only) instance    
    val stmt : Statement = iter.nextStatement()
    return stmt
    
  }
  
  // graph partitioning into schema-, type- and data-components
  def partition(pred:String): String = pred match {
    case x if x == Rules.S_RDFS_SUBCLASS_OF => "SG"
    case x if x == Rules.S_RDFS_SUBPROPERTY_OF => "SG"
    case x if x == Rules.S_RDFS_PROPERTY => "SG"
    case x if x == Rules.S_RDFS_DOMAIN => "SG"
    case x if x == Rules.S_RDFS_RANGE => "SG"
    case x if x == Rules.S_RDF_TYPE => "TG"
    case _ => "DG"
  }
  
  // Triple mapper
  def mapper(line:String): Triples = {
    
    val stmt = getStmt(line) // Get statements
    
    // Literals are simplified to just indicate the datatype
    val obj = if(stmt.getObject().isLiteral()) stmt.getLiteral().getDatatype().toString else stmt.getObject().toString
    
    // Return triple content
    val triple:Triples = Triples(stmt.getSubject().toString, stmt.getPredicate().toString, obj)
    return triple
    
  }
  
  // Transitive closure for subPropertyOf subPropertyOf ... or subClassOf subClassOf
  def addTransitive[A,B](s:Set[(A,B)]) = {
    s ++ (for ((x1,y1) <- s; (x2,y2) <- s if y1 == x2) yield (x1,y2))
  }
  def transitiveClosure[A,B](s:Set[(A,B)]):Set[(A,B)] = {
    val t = addTransitive(s)
    if(t.size == s.size) s else transitiveClosure(t)
  }
  
  // Number stream to be used with URI generator
  val numberStream = Iterator.from(0)
  
  // URI generator (assigning variables)
  def newURI(): String = {
    "d"+numberStream.next
  }
  
  /** MAIN FUNCTION */
  def main(args: Array[String]) {
    
    /*
     * LOAD GRAPH DATA TO DISTINCT DATASETS
     */
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("WeakSummary")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    // For implicit conversions (ie RDDs to DataFrames)
    import spark.implicits._
    
    // Get file path
    val triplePath : String = if(args.length > 0) args(0) else "peel.nt"
    
    // Extract lines from file
    val lines = spark.sparkContext.textFile(triplePath)
    
    // Parse lines to DataSets
    val st = lines.map(mapper).filter(t => t.pred == "SG").toDS().cache()  // Schema Triples
    
    val triples = lines
      .map(mapper)
      .filter(t => t.pred != "SG")  // Data and type Triples
      .toDS()
      .cache()
      
    /*
     * GRAPH SATURATION
     * 
     * Reference:
		 * Gu, R., Wang, S., Wang, F., Yuan, C., & Huang, Y. (2015).
		 * Cichlid: Efficient Large Scale RDFS/OWL Reasoning with Spark. Proceedings 
		 * - 2015 IEEE 29th International Parallel and Distributed Processing Symposium, 
		 * IPDPS 2015, 700–709.
     */

    // Filter schema and load to memory
    
    // RDFS:SubPropertyOf => (s, o)
    val subprop = st
      .filter( t => t.pred == Rules.S_RDFS_SUBPROPERTY_OF )
      .map( t => (t.sub,t.obj) )
      .collect
    // RDFS:SubClassOf => (s, o)
    val subclass = st
      .filter( t => t.pred == Rules.S_RDFS_SUBCLASS_OF )
      .map( t => (t.sub,t.obj) )
      .collect
    // RDFS:Domain => (s, o)
    val domain = st
      .filter( t => t.pred == Rules.S_RDFS_DOMAIN )
      .map( t => (t.sub,t.obj) )
      .collect
    // RDFS:Range => (s, o)
    val range = st
      .filter( t => t.pred == Rules.S_RDFS_RANGE )
      .map( t => (t.sub,t.obj) )
      .collect
    
    // p rdfs:subPropertyOf q &  q rdfs:subPropertyOf r => p rdfs:subPropertyOf r
    val subprops = transitiveClosure(subprop.toSet)
    
    // x rdfs:subClassOf y & y rdfs:subClassOf z => x rdfs:subClassOf z
    val subclasses = transitiveClosure(subclass.toSet)
    
    // Cache broadcast maps
    val sp = spark.sparkContext.broadcast(subprops.toMap)
    val cl = spark.sparkContext.broadcast(subclasses.toMap)
    val dm = spark.sparkContext.broadcast(domain.toMap)
    val rg = spark.sparkContext.broadcast(range.toMap)
    
    // Rule 7: s p o & p rdfs:subPropertyOf q => s q o
    val r7_t = triples
      .filter(t => sp.value.contains(t.pred))
      .map(t => (Triples(t.sub,sp.value(t.pred),t.obj)))
      .persist()
    val r7_out = r7_t.union(triples)
    
    // Rule 2 + 3: 
    // p rdfs:domain x & s p o => s rdf:type x 
    // p rdfs:range x & s p o => o rdf:type x
    val r2_out = r7_out
      .filter(t => dm.value.contains(t.pred))
      .map(t => (t.sub,dm.value(t.pred)))
    //
    val r3_out = r7_out
      .filter(t => rg.value.contains(t.pred))
      .map(t => (t.obj,rg.value(t.pred)))
    // Result
    val tp = triples
      .filter(t => t.pred.equals(Rules.S_RDF_TYPE))
      .map(t => (t.sub,t.obj))
    val out_23 = r2_out.union(r3_out).union(tp)
    
    // Rule 9: s rdf:type x & x rdfs:subClassOf y => s rdf:type y
    val r9_out = out_23
      .filter(t => cl.value.contains(t._2))
      .map(t => (t._1,cl.value(t._2)))
      
    val tpall = out_23
      .union(r9_out)
      .map(t =>(Triples(t._1,Rules.S_RDF_TYPE,t._2)))
      .union(r7_t)
    
    // Define two new datasets containing data / type triples
    val dt = triples
      .union(tpall)
      .filter(t => partition(t.pred) == "DG")
      .distinct()
      
    val tt = triples
      .union(tpall)
      .filter(t => partition(t.pred) == "TG")
      .distinct()

    
    /*
     * CREATE BASELINE SUMMARY
     */

    // Every property group initially gets a distinct URI for the source and the target
    // DNT2: if s p1 o1, s p2 o2 ∈ G, then S(p1) = S(p2)
    // DNT3: if s1 p1 o, s2 p2 o ∈ G, then T(p1) = T(p2)
    val cliqueVars = dt
      .map( t => t.pred )              // filter out everything but the predicates
      .distinct()                      // get only distinct values
      .map( t => (t,(newURI,newURI)) ) // make a new dataset with (property, (sourceURI,targetURI))
      .collect()
    
    // Returns something like this:
    //+--------------------------------------------+---------+
    //|_1                                          |_2       |
    //+--------------------------------------------+---------+
    //|http://purl.org/ontology/mo/recording_of    |[d0,d1]  |
    //|http://purl.org/ontology/mo/engineered      |[d2,d3]  |
    //|http://purl.org/ontology/mo/engineer        |[d4,d5]  |
      
    // Turn the clique variables into broadcasts that can be checked against on all nodes
    val cliqueVar = spark.sparkContext.broadcast(cliqueVars.toMap)
    
    // Map source URIs to clique variables
    val sourceCliques = dt
      .map( t => (t.sub,cliqueVar.value(t.pred)._1) )     // make a new dataset with (sub, sourceVar)
      .collect()

    //+------------------------------------------------------------------+---+
		//|_1                                                                |_2 |
		//+------------------------------------------------------------------+---+
		//|http://dbtune.org/bbc/peel/artist/03d27565f152e2f64ba095dfb48f8818|d10|
		//|http://dbtune.org/bbc/peel/artist/0622cef09f17ce30c809144898bc198c|d38|
		//|http://dbtune.org/bbc/peel/artist/06231cb5bfb3a2ca7d5b861d317a9763|d38|

    // Turn the sourceCliques into a broadcast that can be checked against on all nodes
    val sourceClique = spark.sparkContext.broadcast(sourceCliques.toMap)
    
    // Run through the dataset and assign clique variables to the targets
    val targetURIs = dt
      .map( t => TriplesTgt( t.sub,t.pred,t.obj,cliqueVar.value(t.pred)._2 ) )

    //+------------------------------------------------------------------+------------------------------------------+---------------------------------------------------------------------------+----+
    //|sub                                                               |pred                                      |obj                                                                        |varT|
    //+------------------------------------------------------------------+------------------------------------------+---------------------------------------------------------------------------+----+
    //|http://dbtune.org/bbc/peel/artist/03d27565f152e2f64ba095dfb48f8818|http://purl.org/ontology/mo/performed     |http://dbtune.org/bbc/peel/perf_ins/03d27565f152e2f64ba095dfb48f8818       |d11 |
    //|http://dbtune.org/bbc/peel/artist/0622cef09f17ce30c809144898bc198c|http://xmlns.com/foaf/0.1/name            |Datatype[http://www.w3.org/2001/XMLSchema#string -> class java.lang.String]|d41 |
    //|http://dbtune.org/bbc/peel/artist/06231cb5bfb3a2ca7d5b861d317a9763|http://xmlns.com/foaf/0.1/name            |Datatype[http://www.w3.org/2001/XMLSchema#string -> class java.lang.String]|d41 |
    
    // Assign clique variables to the sources with the following rules:
    // If subject is contained in the target clicque:
	  //   use T(p)
    // Else:
	  //   use S(p)
    // DNT4: if s p1 o1, o1 p2 o2 ∈ G, then T(p1) = S(p2)
    val dataS = targetURIs
      .map( t => TriplesVars( t.sub,{if(sourceClique.value.contains(t.sub)) sourceClique.value(t.sub) else cliqueVar.value(t.pred)._1},t.pred,t.obj,t.varT) )
      .dropDuplicates("varS","varT")  // Remove duplicate representations of a    var1 prop var2
      .collect()
      
    // +-----------------------------------------------------------------------+----+-------------------------------------------+---------------------------------------------------------------------------+----+
    //|sub                                                                    |varS|pred                                       |obj                                                                        |varT|
    //+-----------------------------------------------------------------------+----+-------------------------------------------+---------------------------------------------------------------------------+----+
    //|http://dbtune.org/bbc/peel/recording/2365                              |d44 |http://purl.org/ontology/mo/produced_signal|http://dbtune.org/bbc/peel/signal/2365                                     |d25 |
    //|http://dbtune.org/bbc/peel/perf_ins/0b5b5a7dda785f552d4a12a1368c45be   |d30 |http://purl.org/ontology/mo/instrument     |Datatype[http://www.w3.org/2001/XMLSchema#string -> class java.lang.String]|d7  |
    //|http://dbtune.org/bbc/peel/perf_work/12117                             |d34 |http://purl.org/ontology/mo/recorded_as    |http://dbtune.org/bbc/peel/signal/12117                                    |d23 |

    // Map obj URIs to clique variables
    val targetCliques = dataS
      .map( t => (t.obj,t.varT) )     // make a new dataset with (obj, targetVar)
      
    // Turn the targetCliques into a broadcast that can be checked against on all nodes (should be smaller than the source broadcast since the dataset is now smaller)
    val targetClique = spark.sparkContext.broadcast(targetCliques.toMap)

    /*
     * DT
     */
    // DT1: If s p o, s τ c are in G, then S(p) τ c is in BG
    val sourceTypes = tt
      .filter(t => sourceClique.value.contains(t.sub)) //subject should  be contained in the source clique
      .map( t => Triples(sourceClique.value(t.sub),t.pred,t.obj) )  // (var rdf:type c)
      .distinct()
      .collect()

    val targetTypes = tt
      .filter(t => targetClique.value.contains(t.sub)) //subject should  be contained in the target clique
      .map( t => Triples(targetClique.value(t.sub),t.pred,t.obj) )  // (var rdf:type c)
      .distinct()
      .collect()
    
    // Weld it all together
    val summary = dataS
      .map(t => Triples(t.varS,t.pred,t.varT))
      .union(sourceTypes)
      .union(targetTypes)
    
    /*
     * WRITE RESULT TO FILE
     */
      
    val file = "summary.nt"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- summary) {
       writer.write("<"+x.sub+"> <"+x.pred+"> <"+x.obj+"> .\n")
    }
    writer.close()
    
    println("Summary saved to summary.nt")
   
    spark.stop()
  }
}