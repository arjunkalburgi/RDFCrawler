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
		doo.setDepth(2); 
		doo.crawlSubject("Albert Einstein");

	}
	
	private String inputPath; 
	private String propertyPath; 
	private int crawlDepth = 2; 
	private Dataset dataset;
	private boolean property_else; 
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
		crawlSubject(subject, 0, tdb); //starting depth
		// Crawling is complete. Save data.
		//tdb.tdbdump
		//RDFDataMgr.write(System.out, tdb, RDFFormat.RDFXML); 
		//tdb.write(new File("./src/output.xml"));
		tdb.write(System.out, "RDF/XML"); 
	}
	
	public void draw() {
		
	}

	private void crawlSubject(String subject, int depth, Model tdb) {
		if (depth < crawlDepth) {

			//Keep tdb model name here will not keep all the crawling for one resource in one set.   
			//Model tdb = dataset.getNamedModel(subject);

            // get for s' data
            String q = "PREFIX dbres: <http://dbpedia.org/resource/> " +
                    "SELECT ?b ?x WHERE {" +
                    " dbres:" + geturistring(subject) + " ?b ?x . " +
                    "} ";
            Query query = QueryFactory.create(q);
            ResultSet r = null;
            QueryExecution qex = QueryExecutionFactory.sparqlService("http://dbpedia.org/sparql", query);
            r = qex.execSelect();

            Resource sub = ResourceFactory.createResource("http://dbpedia.org/resource/" + geturistring(subject));
            while (r.hasNext()) {
                QuerySolution t = r.nextSolution();
                System.out.println(t.getResource("b".toString()) + " " + property_else);
                System.out.println("Error is that it's not finding the right properties notice all the properties are false."); 
                if (properties.containsKey(t.getResource("b".toString())) || property_else == true) {
                	 
                	/*Property p = ResourceFactory.createProperty(t.getResource("b").toString());
	                RDFNode o = t.get("x");
	                //TODO store thing in tdb with subject as graph name
	                tdb.add(sub, p, o);
	                System.out.println(sub.toString() + " " + p.toString() + " " + o.toString());
                	//System.out.println(o.toString()); 
	                
	                //TODO call crawlSubject with thing and depth+1
	                crawlSubject(o.toString(), depth+1, tdb);*/ 
                }
            }
		}
	}
	
    private static String geturistring(String s) {
        return s.replaceAll(" ", "_"); 
    }
    
    private void setHash() {
    	
		try (BufferedReader br = new BufferedReader(new FileReader(this.propertyPath))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		       if (line.split(" ")[1] == "else") {
		    	   if (line.split(" ")[0] == "+") {
		    		   this.property_else = true; 
		    	   } else this.property_else = false;  
		       } else {
		    	   if (line.split(" ")[0] == "+") {
			    	   properties.put(line.split(" ")[1], true); 
			       } else properties.put(line.split(" ")[1], false); 
		       }
		    }
		}
		catch (Exception e) {
			System.out.println("There is a problem reading the properties.txt file.");
			System.out.println(e);
		}
    }
}
