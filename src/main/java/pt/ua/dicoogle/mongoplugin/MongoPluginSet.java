/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pt.ua.dicoogle.mongoplugin;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.NoSuchElementException;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import net.xeoh.plugins.base.annotations.PluginImplementation;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pt.ua.dicoogle.sdk.PluginBase;
import pt.ua.dicoogle.sdk.settings.ConfigurationHolder;

/**
 *
 * @author Louis
 */
@PluginImplementation
public class MongoPluginSet extends PluginBase {

	private static final Logger log = LogManager.getLogger(MongoQuery.class.getName());
	
    private String host;
    private int port;
    protected static MongoClient mongoClient = null;
    private MongoQuery plugQuery;
    private MongoIndexer plugIndexer;
    private MongoStorage plugStorage;
	private String collectionName;
	private String dbName;
	private DBCollection defaultCollection;
	private URI location;
    private static String hostKey = "host";
    private static String portKey = "port";
    private static final String dbNameKey = "database";
    private static final String collectionNameKey = "collection";
    
    public MongoPluginSet() {
       System.out.println("INIT-->MongoDb plugin");
        
        plugQuery = new MongoQuery();
        this.queryPlugins.add(plugQuery);
        plugIndexer = new MongoIndexer();
        this.indexPlugins.add(plugIndexer);
        plugStorage = new MongoStorage();
        this.storagePlugins.add(plugStorage);
    }

    @Override
    public String getName() {
        return "mongoplugin";
    }

    @Override
    public void setSettings(ConfigurationHolder settings) {
        this.settings = settings;
        HierarchicalConfiguration cnf = this.settings.getConfiguration();
        
        this.host = cnf.getString(hostKey);
        if(this.host == null)
        {
        	this.host = "localhost";
        	cnf.setProperty(hostKey, this.host);        	
        }
        
        this.port = cnf.getInteger(portKey, 9999);
        if(this.port == 9999)
        	cnf.setProperty(portKey, 9999);
        
        try {
            if (mongoClient == null) {
                mongoClient = new MongoClient(host, port);
            }
        } catch (UnknownHostException e) {
            return ;
        } catch (MongoException e) {
            return;
        }
        
        cnf.setThrowExceptionOnMissing(true);
        
        try{
        	this.dbName = cnf.getString(dbNameKey);
        }catch(NoSuchElementException ex){
        	this.dbName = "defaultDB";
        	cnf.setProperty(dbNameKey, this.dbName);
        }
        
        try{
        	this.collectionName = cnf.getString(collectionNameKey);
        }catch(NoSuchElementException ex){
        	this.collectionName = "defaultCollection";
        	cnf.setProperty(collectionNameKey, this.collectionName);
        }        
        
        defaultCollection = mongoClient.getDB(dbName).getCollection(collectionName); 
        
		try {
			this.location = new URI("mongodb" + "://" + host + ":" + port + "/"
					+ dbName + "/");
		} catch (URISyntaxException e) {			
			log.error("COULD NOT INITIATE BASE URI: PLUGIN SHUTDOWN", e);
			return;
		}        
        
        this.plugIndexer.setSettings(settings);       
        this.plugIndexer.setMongoClient(defaultCollection);
        
        this.plugQuery.setSettings(settings); 
        this.plugQuery.setMongoClient(defaultCollection);
        
        this.plugStorage.setSettings(settings); 
        this.plugStorage.setMongoClient(mongoClient.getDB(dbName), this.location);       
    }

    @Override
    public ConfigurationHolder getSettings() {
        return settings;
    }
    
}
