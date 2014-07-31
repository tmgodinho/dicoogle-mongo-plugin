/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pt.ua.dicoogle.mongoplugin;

import com.mongodb.gridfs.GridFSDBFile;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import pt.ua.dicoogle.sdk.StorageInputStream;

/**
 *
 * @author Louis
 */
public class MongoStorageInputStream implements StorageInputStream {

    private URI uri = null;
    private GridFSDBFile file;

    public MongoStorageInputStream(URI uri, GridFSDBFile file) {
        this.uri = uri;
        this.file = file;
    }

    public URI getURI() {
        return uri;
    }

    public InputStream getInputStream() {
    	return file.getInputStream();
    }

	public long getSize() throws IOException {
		return file.getLength();
	}
}
