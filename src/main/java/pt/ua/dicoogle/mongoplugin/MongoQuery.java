/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pt.ua.dicoogle.mongoplugin;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.net.URI;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static pt.ua.dicoogle.mongoplugin.MongoPluginSet.mongoClient;
import pt.ua.dicoogle.sdk.QueryInterface;
import pt.ua.dicoogle.sdk.datastructs.SearchResult;
import pt.ua.dicoogle.sdk.settings.ConfigurationHolder;

/**
 *
 * @author Louis
 */
class MongoQuery implements QueryInterface {

	private static final Logger log = LogManager.getLogger(MongoQuery.class.getName());
	
    private DBCollection collection;
    private boolean isEnable;
    
    private ConfigurationHolder settings;
	private final static String enableQueryKey = "useQuery";

    public MongoQuery() {
    }

    public Iterable<SearchResult> query(String query, Object... os) {
        Iterable<SearchResult> result;
        if (!isEnable || mongoClient == null) {
            return null;
        }
        MongoQueryUtil mongoQuery = new MongoQueryUtil(query);
        List<DBObject> resultDBobjs = mongoQuery.processQuery(collection);
        result = MongoUtil.getListFromResult(resultDBobjs, (float) 0.0);
        return result;
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
        	this.isEnable = cnf.getBoolean(enableQueryKey);
        }catch(NoSuchElementException ex){
        	this.isEnable = true;
        	cnf.setProperty(enableQueryKey, this.isEnable);
        }
    }

    public ConfigurationHolder getSettings() {
        return settings;
    }

	public void setMongoClient(DBCollection defaultCollection) {
		this.collection = defaultCollection;
	}
}
