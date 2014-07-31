/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pt.ua.dicoogle.mongoplugin;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.dcm4che2.data.DicomObject;
import org.dcm4che2.data.Tag;
import org.dcm4che2.io.DicomInputStream;
import org.dcm4che2.io.DicomOutputStream;

import static pt.ua.dicoogle.mongoplugin.MongoPluginSet.mongoClient;
import pt.ua.dicoogle.sdk.StorageInputStream;
import pt.ua.dicoogle.sdk.StorageInterface;
import pt.ua.dicoogle.sdk.settings.ConfigurationHolder;

/**
 *
 * @author Louis
 */
class MongoStorage implements StorageInterface {

    private DB db;
    private ConfigurationHolder settings;
    private URI location;
    private String dbName;
    private String host;
    private int port;
    private boolean isEnable;
	private static final String enableStorageKey = "useStorage";

    private static long nbFiles = 0;
    private static final long NB_FILES = 15733;

    public MongoStorage() {
    }

    public String getScheme() {
        return this.getName() + "://" + host + ":" + port + "/" + dbName + "/";
    }

    public boolean handles(URI pUri) {
        MongoURI uri = new MongoURI(pUri);
        return uri.verify();
    }

    public Iterable<StorageInputStream> at(URI pUri) {
        MongoURI uri = new MongoURI(pUri);
        if (!isEnable || !uri.verify() || mongoClient == null) {
            return null;
        }

        ArrayList<StorageInputStream> list = new ArrayList<StorageInputStream>();
        
        uri.getInformation();        
        GridFS fs = new GridFS(this.db);
        String fileName = uri.getFileName();
        GridFSDBFile in = fs.findOne(fileName);
        
        if(in == null)
        	return list;
        
        MongoStorageInputStream MongoStorageIn = new MongoStorageInputStream(pUri, in);
        list.add(MongoStorageIn);
        return list;
    }

    public URI store(DicomObject dicomObject) {
        if (!isEnable || mongoClient == null || dicomObject == null) {
            return null;
        }
        nbFiles++;
        String fileName = dicomObject.get(Tag.SOPInstanceUID).getValueAsString(dicomObject.getSpecificCharacterSet(), 0);
        if(nbFiles > NB_FILES){
            fileName += "("+(nbFiles/NB_FILES)+")";
            dicomObject.putString(Tag.SOPInstanceUID, dicomObject.vrOf(Tag.SOPInstanceUID), fileName);
        }
        URI uri;
        try {
            uri = new URI(this.location + fileName);
        } catch (URISyntaxException e) {
            System.out.println("Error : URISyntaxException");
            return null;
        }
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            DicomOutputStream dos = new DicomOutputStream(os);
            dos.writeDicomFile(dicomObject);
            GridFS saveFs = new GridFS(db);
            GridFSInputFile ins = saveFs.createFile(os.toByteArray());
            ins.setFilename(fileName);
            ins.save(ins.getChunkSize());
            os.close();
        } catch (IOException ex) {
            Logger.getLogger(MongoStorage.class.getName()).log(Level.SEVERE, null, ex);
        }
        return uri;
    }

    public URI store(DicomInputStream stream) throws IOException {
        if (!isEnable || mongoClient == null || stream == null) {
            return null;
        }
        return this.store(stream.readDicomObject());
    }

    public void remove(URI pUri) {
        MongoURI uri = new MongoURI(pUri);
        if (!isEnable || !uri.verify() || mongoClient == null) {
            return;
        }
        uri.getInformation();
        GridFS removeFS = new GridFS(db);
        removeFS.remove(uri.getFileName());
        System.out.println("Remove done");
    }

    public String getName() {
        return "mongodb";
    }

    public boolean enable() {
        if (mongoClient == null || this.settings == null) {
            return false;
        }
        try {
            if (mongoClient == null) {
                mongoClient = new MongoClient(host, port);
            }
        } catch (UnknownHostException e) {
            return false;
        } catch (MongoException e) {
            return false;
        }
        try {
            location = new URI("mongodb" + "://" + host + ":" + port + "/" + dbName + "/");
        } catch (URISyntaxException e) {
            return false;
        }
        isEnable = true;
        return true;
    }

    public boolean disable() {
        mongoClient.close();
        isEnable = false;
        return true;
    }

    public boolean isEnabled() {
        return isEnable;
    }

    public void setSettings(ConfigurationHolder stngs) {
        this.settings = stngs;
        
        HierarchicalConfiguration cnf = this.settings.getConfiguration();
        cnf.setThrowExceptionOnMissing(true);
        
		try{
        	this.isEnable = cnf.getBoolean(enableStorageKey);
        }catch(NoSuchElementException ex){
        	this.isEnable = true;
        	cnf.setProperty(enableStorageKey, this.isEnable);
        }
        
        
    }
    
	public void setMongoClient(DB database, URI location) {
		this.db = database;        
		this.location = location;
		
		DBCollection collection =  db.getCollection("fs.files");
        DBObject keys = new BasicDBObject("filename", 1);
        DBObject options = new BasicDBObject();
        options.put("background", true);
        collection.createIndex(keys, options);
	}

    public ConfigurationHolder getSettings() {
        return this.settings;
    }
}
