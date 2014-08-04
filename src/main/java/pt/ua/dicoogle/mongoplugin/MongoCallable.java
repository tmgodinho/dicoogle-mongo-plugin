/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pt.ua.dicoogle.mongoplugin;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dcm4che2.data.DicomElement;
import org.dcm4che2.data.DicomObject;
import org.dcm4che2.io.DicomInputStream;

import pt.ua.dicoogle.sdk.StorageInputStream;
import pt.ua.dicoogle.sdk.datastructs.DocumentIndexReport;
import pt.ua.dicoogle.sdk.datastructs.Report;
import pt.ua.dicoogle.sdk.datastructs.TaskIndexReport;
import pt.ua.dicoogle.sdk.utils.TagValue;
import pt.ua.dicoogle.sdk.utils.TagsStruct;

/**
 *
 * @author Louis
 */
public class MongoCallable implements Callable<Report> {

	private static final Logger log = LogManager.getLogger(MongoCallable.class.getName());
	private static final Logger audit = LogManager.getLogger("audit.Mongo");
	
    private DBCollection collection;
    private Iterable<StorageInputStream> itrblStorageInputStream = null;
   
    public MongoCallable(Iterable<StorageInputStream> itrbl, DBCollection pCollection) {
        super();
        this.itrblStorageInputStream = itrbl;
        this.collection = pCollection;
    }

    public Report call() throws Exception {
        if (itrblStorageInputStream == null) {
            log.error("NO FILES TO INDEX");
            return null;
        }
        
        log.info("Started Index Task: "+this.hashCode());
        int i = 1;
        
        TaskIndexReport taskReport = new TaskIndexReport();        
		taskReport.start();
        
        for (StorageInputStream stream : itrblStorageInputStream) {
        	log.info("Started Indexing: {},{},{}",this.hashCode(), i, stream.getURI());

        	DocumentIndexReport dReport = new DocumentIndexReport(stream.getURI().toString());
        	dReport.start();
            try {            	
            	dReport.getRetrieveObjectTime().start();
            	
                DicomInputStream dis = new DicomInputStream(stream.getInputStream());
                DicomObject dicomObj = dis.readDicomObject();
                dis.close();
                
                dReport.getRetrieveObjectTime().stop();
                
                if(dicomObj == null)
                	throw new IOException();
                
                //String SOPInstanceUID = dicomObj.get(Tag.SOPInstanceUID).getValueAsString(dicomObj.getSpecificCharacterSet(), 0);
                dReport.getAssembleDocumentTIme().start();
                
                HashMap<String, Object> map = retrieveHeader(dicomObj);                
                map.put("uri", stream.getURI().toString());
                map.put("FileSize", stream.getSize());                
                BasicDBObject obj = new BasicDBObject(map);
                
                dReport.getAssembleDocumentTIme().stop();
                
                dReport.getStoreInDatabaseTime().start();
                
                collection.insert(obj);
                
                dReport.getStoreInDatabaseTime().stop();                
                log.info("Finished Indexing: {},{},{},{}",this.hashCode(), i, stream.getURI(), taskReport);
            } catch (IOException ex) {
            	dReport.setSuccessfull(false);
            	log.error("ERROR Indexing: {},{},{}",this.hashCode(), i, stream.getURI(), ex);
            }
            dReport.stop();
            taskReport.addReport(dReport);
            i++;
        }
        taskReport.stop();
        audit.info(taskReport);
        log.info("Finished Index Task: {},{}",this.hashCode(), taskReport);
        return taskReport;
    }
        
    private HashMap<String, Object> retrieveHeader(DicomObject dicomObject) {
        HashMap<String, Object> map = new HashMap<String, Object>();
        
        HashMap<Integer, TagValue> arr = new HashMap<Integer, TagValue>(TagsStruct.getInstance().getDimFields());
        arr.putAll(TagsStruct.getInstance().getManualFields());
        
        for(Integer key : arr.keySet()){        	
        	DicomElement e = dicomObject.get(key);  
        	if(e != null){
        		String tagValue = e.getValueAsString(dicomObject.getSpecificCharacterSet(), 0);
        	
        		if(tagValue != null){
	        		Object obj;
	        		try {
	                    obj = Double.parseDouble(tagValue);
	                } catch (NumberFormatException ex) {
	                    obj = tagValue.trim();
	                }
	        		
	        		map.put(arr.get(key).getAlias(), obj);
        		}else{
        			map.put(arr.get(key).getAlias(), "");    
        		}
        	}else{
        		map.put(arr.get(key).getAlias(), "");        		
        	}        	
        }
        return map;
    }
}
