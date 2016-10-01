import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.util.*;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

// run in Window can not handle international chars, export jar and run it in Linux
public class FileProducer {
	
	private static String ROOT_DIR = null;
	org.apache.kafka.clients.producer.Producer<String, String> producer = null;
	private String topic = null;
	private Integer index = 0;
	private Integer committed_count = 0;
	private Integer MAX_ACK_WAIT_LOOPS = 20;
	
	private Set<Integer> sendSet = new HashSet<Integer>(1048576);
	private Set<Integer> committedSet = new HashSet<Integer>(1048576);
	static Logger log = Logger.getLogger(FileProducer.class.getName());
	
	public static void main(String[] args) {		
		
		if(args.length != 2)
		{
			System.err.println("usage: <data file dir prefix> <data folder list>");
			System.err.println("example: FileProducer /home/mxu/SurveyBackFillFiles/ dir_list.txt");
			return;
		}
		//String currentDir = System.getProperty("user.dir");
		ROOT_DIR = args[0];
		if(!ROOT_DIR.endsWith(File.separator))
		{
			ROOT_DIR += File.separator;
		}
		String inputDirFile = args[1];
		
		//run in Window can not handle international chars, copy jar and run it in Linux
		//Charset charset = Charset.defaultCharset();
		//System.setProperty("file.encoding", "UTF-8");
		
		FileProducer fileProducer= new FileProducer();
		fileProducer.run(inputDirFile);			
	}
	
	FileProducer()
	{
       createProducer();		
	}
	
	public void run(String inputDirFile)
	{
		try
		{			
			String fullInputFile = inputDirFile;					
			File inputFile = new File(fullInputFile);
			if(inputFile.exists())
			{
			  try
			  {
			   System.out.println("input file: " + fullInputFile);
			   log.info("input file: " + fullInputFile);
			   BufferedReader br = new BufferedReader(new FileReader(fullInputFile));
			   for(String line; (line = br.readLine()) != null; ) 
			   {
				   if(line.isEmpty() || line.trim().startsWith("#"))
					   continue;
				   
				   //get files in this dir
				   System.out.println("processing dir: " + line);
				   log.info("processing dir: " + line);
				   File dir = new File(ROOT_DIR +line);
				   String[] files = dir.list();				   
				   try
					{
					  //read context of each file
					  if(files ==null)
					  {
						  throw new Exception("dir " + ROOT_DIR +line +  " does not exist");
					  }
					  for(String file : files)
					  {	
						long start = System.currentTimeMillis();
						
						String fullPathFile = ROOT_DIR + line + File.separator + file;  
						System.out.println("reading file: " + file);
						log.info("reading file: " + file);
						
					    index = 0;
					    committed_count = 0;
					    sendSet.clear();
					    committedSet.clear();
				        BufferedReader brc = new BufferedReader(new FileReader(fullPathFile));
					    for(String message; (message = brc.readLine()) != null; ) 
					    {
						  ++index;
						  sendSet.add(index);
						  producer.send(new ProducerRecord<String, String>(topic, index.toString(), message),
								  new Callback() {
			                      public void onCompletion(RecordMetadata metadata, Exception e) {
			                       if(e != null)
			                           e.printStackTrace();
			                           ++committed_count;
			                           committedSet.add(committed_count);
			                           //System.out.printf("index=%d, offset =%d  ", index, metadata.offset());
			                   }
						  });
						  //producer.send(new ProducerRecord<String, String>(topic, i.toString(), message));
				        }
					    
					    //wait to let ack catch up
					    //set ack to -1 still needs to wait
					    //Thread.sleep(index);
					    
					    //max try = 10
					    Integer tries = 0;
					    do
					    {
					      ++tries;
					      if(index.intValue() != committed_count.intValue() )
					      {
					    	System.err.printf("missing ack for %s\n", file);
					        log.error(String.format("missing ack for %s", file));
					        
					    	//sleep and let it catch up					    	
					    	System.err.printf("sleep for 1 sec for %d times\n", tries);					    	
					    	log.info(String.format("sleep 1 sec for %d times\n", tries));				    	
					    	
					    	Thread.sleep(1000);
					      }
					      //System.out.printf("\n");
					    }
					    while(tries <= MAX_ACK_WAIT_LOOPS);
					    
					    //after MAX_ACK_WAIT_LOOPS tries
					    if(index.intValue() != committed_count.intValue() )
					    {
					    	System.err.printf("missing ack for %s\n", file);
					    	log.error(String.format("missing ack for %s\n", file));
					    	sendSet.removeAll(committedSet);					    	
					    	for(Integer m : sendSet)
					    	{
					    	  System.out.printf("%d,", m);
					    	  log.info(String.format("%d", m));
					    	}
					    }
					    System.out.printf("\n");
					    log.info("");
					    
					    brc.close();
					    
					    long end = System.currentTimeMillis();
						System.out.printf("file %s has %d line, time elapsed %d seconds\n", file, index, (end - start)/1000);
						log.info(String.format("file %s has %d line, time elapsed %d seconds\n", file, index, (end - start)/1000));
						
					  }
					  
					}
					catch(Exception e)
					{
						e.printStackTrace();
						log.error(e.getMessage());
						
					}		
		       }
			   br.close();
			    
			  }
			  catch(Exception e)
			  {
				  e.printStackTrace();
				  log.error(e.getMessage());
			  }
			 }		 
			 else
			 {
			   System.err.println("input file not found " + fullInputFile);
			   log.error("input file not found " + fullInputFile);
			 }
	}
	finally
	{
		if(producer != null)
			producer.close();
	}
				
	}
		
	private void createProducer()
	{
        try {			
			
			Properties props = new Properties();
			String propFileName = "config.properties";
			InputStream inputStream = FileProducer.class.getClassLoader().getResourceAsStream(propFileName);
			if (inputStream != null) {
				props.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}			
				
			props.getProperty("bootstrap.servers");
			
			props.put("serializer.class", "kafka.serializer.StringEncoder");
			
			String acks = props.getProperty("request.required.acks");			
			props.put("acks", acks);
			
			String retries = props.getProperty("retries");			
			props.put("retries", retries);
			
			//to avoid data loss
			props.put("max.in.flight.requests.per.connection", "1");
			props.put("block.on.buffer.full", "true");		
			
			props.put("batch.size", 16384);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("linger.ms", 2);
			props.put("request.timeout.ms", 1200000);
			props.put("timeout.ms", 3000000);
			
			
			topic = props.getProperty("topic");
			String maxAckWaitLoop = props.getProperty("max.ack.wait.loops");
			if(maxAckWaitLoop != null)
			  MAX_ACK_WAIT_LOOPS = Integer.parseInt(maxAckWaitLoop);

			producer = new KafkaProducer<String, String>(props);				

			
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}		
	}
}