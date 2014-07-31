/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pt.ua.dicoogle.mongoplugin;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import java.net.URI;
import java.util.ArrayList;
import java.util.NoSuchElementException;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.dcm4che2.data.Tag;

import pt.ua.dicoogle.sdk.IndexerInterface;
import pt.ua.dicoogle.sdk.StorageInputStream;
import pt.ua.dicoogle.sdk.datastructs.Report;
import pt.ua.dicoogle.sdk.settings.ConfigurationHolder;
import pt.ua.dicoogle.sdk.task.Task;


/**
 *
 * @author Louis
 */
public class MongoIndexer implements IndexerInterface {

    private DBCollection collection;
    private boolean isEnable;
    private ConfigurationHolder settings = null;
    private MongoClient mongoClient;
    
    //Configuration Fields

    private static final String enableIndexerKey = "useIndexer";

    //private static String fileName = "log.txt";
    
    public MongoIndexer() {
    }

    public Task<Report> index(StorageInputStream stream) {
        ArrayList<StorageInputStream> itrbl = new ArrayList<StorageInputStream>();
        itrbl.add(stream);
        MongoCallable c = new MongoCallable(itrbl, collection);
        Task<Report> task = new Task<Report>(c);
        return task;
    }
   
    public Task<Report> index(Iterable<StorageInputStream> itrbl) {
        MongoCallable c = new MongoCallable(itrbl, collection);
        Task<Report> task = new Task<Report>(c);
        return task;
    }

    public boolean unindex(URI pUri) {
        MongoURI uri = new MongoURI(pUri);
        if (!isEnable || mongoClient == null) {
            return false;
        }
        Dictionary dicoInstance = Dictionary.getInstance();
        DBObject query = new BasicDBObject();
        query.put(dicoInstance.tagName(Tag.SOPInstanceUID), uri.getFileName());
        collection.findAndRemove(query);
        return true;
    }

    public String getName() {
        return "mongodb";
    }

    public boolean enable() {
        if (mongoClient == null || this.settings == null) {
            return false;
        }
        isEnable = true;
        return true;
    }

    public boolean disable() {
        isEnable = false;
        return true;
    }

    public boolean isEnabled() {
        return isEnable;
    }

    public void setSettings(ConfigurationHolder settings) {
    	this.settings = settings;
        
        HierarchicalConfiguration cnf = this.settings.getConfiguration();
        cnf.setThrowExceptionOnMissing(true);
        
        try{
        	this.isEnable = cnf.getBoolean(enableIndexerKey);
        }catch(NoSuchElementException ex){
        	this.isEnable = true;
        	cnf.setProperty(enableIndexerKey, this.isEnable);
        }               
    }

    public ConfigurationHolder getSettings() {
        return this.settings;
    }

	public void setMongoClient(DBCollection defaultCollection) {
		this.collection = defaultCollection;
        
		DBObject keys = new BasicDBObject();
        keys.put("SOPInstanceUID", 1);
        keys.put("SeriesInstanceUID", 1);
        DBObject options = new BasicDBObject("background", true);
        collection.createIndex(keys, options);
		
	}
	
	
	//TODO: EXPERIMENTAL!!! WARNING!!!
}
