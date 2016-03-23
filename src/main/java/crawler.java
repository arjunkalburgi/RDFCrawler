import java.io.*;
import java.util.HashMap;

import org.apache.jena.riot.RDFFormat;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.tdb.*;

public class crawler {
	public static void main(String[] args) throws FileNotFoundException, IOException {
		String iP = "./input.txt"; 
		String pP = "./properties.txt"; 
		
//		crawler foo = new crawler(iP, pP);
//		foo.setDepth(2); 
//		foo.crawlFile();
		
		crawler doo = new crawler(pP); 
		doo.setDepth(1); 
		doo.crawlSubject("Albert Einstein");
		doo.draw();

	}
	
	private String inputPath; 
	private String propertyPath; 
	private int crawlDepth = 2; 
	private Dataset dataset;
	private HashMap<String, Boolean> properties = new HashMap<String, Boolean>();

	
	public crawler(String input, String property) {
		inputPath = input;
		propertyPath = property; 
		setHash(); 
		
		dataset = TDBFactory.createDataset("./src/rdfcrawlerDB");
	}
	
	public crawler(String property) {
		propertyPath = property; 
		setHash(); 
		
		dataset = TDBFactory.createDataset("./src/rdfcrawlerDB");
	}
	
	public void setDepth(int d) {
		crawlDepth = d; 
	}
	
	public void crawlFile() {
		try (BufferedReader br = new BufferedReader(new FileReader(this.inputPath))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		       crawlSubject(line); 
		    }
		} 
		catch (Exception e) {
			System.out.println("There is a problem reading the input.txt file.");
		}
	}
	
	public void crawlSubject(String subject) {
		Model tdb = dataset.getNamedModel(subject);
		crawlSubject(subject, 0, tdb); //0 is starting depth
		// Crawling is complete. Save data.
		//tdb.write(System.out, "RDF/XML"); 
		
	}
	
	public void draw() {
		dataset.asDatasetGraph();
	}

	private void crawlSubject(String subject, int depth, Model tdb) {
		if (depth < crawlDepth) {
			// query
			String q = "PREFIX dbres: <http://dbpedia.org/resource/> " +
		    		"SELECT ?property ?object WHERE {" +
		            " dbres:" + geturistring(subject) + " ?property ?object . " +
		            "} ";
		    Query query = QueryFactory.create(q);
		    ResultSet r = null;
		    QueryExecution qex = QueryExecutionFactory.sparqlService("http://dbpedia.org/sparql", query);
		    r = qex.execSelect();
		
		    // search through query 
		    Resource sub = ResourceFactory.createResource("http://dbpedia.org/resource/" + geturistring(subject));
		    while (r.hasNext()) {
		    	QuerySolution t = r.nextSolution();
		        if (properties.containsKey(concatenateuristring(t.getResource("property".toString()).toString()))) {
		        	Property prop = ResourceFactory.createProperty(t.getResource("property".toString()).toString());
		        	RDFNode obj = t.get("object".toString());
		            
		            //TODO store thing in tdb with subject as graph name
		            tdb.add(sub, prop, obj);
		        	
		            //TODO call crawlSubject with thing and depth+1
		            crawlSubject(concatenateuristring(obj.toString()), depth+1, tdb); 
		        }
		    }
		}
	}
	
    private static String geturistring(String s) {
        return s.replaceAll(" ", "_"); 
    }
    
    private static String concatenateuristring(String s) {
    	return s.substring(s.indexOf("/", 20) + 1);
    }
    
    private void setHash() {
    	
		try (BufferedReader br = new BufferedReader(new FileReader(this.propertyPath))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	properties.put(line, true); 
		    }
		}
		catch (Exception e) {
			System.out.println("There is a problem reading the properties.txt file.");
			System.out.println(e);
		}
    }
}
