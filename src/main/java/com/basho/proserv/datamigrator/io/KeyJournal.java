package com.basho.proserv.datamigrator.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Iterator;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.pbc.RiakObject;

import com.google.protobuf.ByteString;

public class KeyJournal implements Iterable<Key> {
	public enum Mode { READ, WRITE }
	
	private final Mode mode;
	private final BufferedOutputStream writer;
	private final BufferedInputStream reader;
	private boolean closed = false;
	
	public KeyJournal(File path, Mode mode) {
		if (path == null) {
			throw new IllegalArgumentException("path cannot be null");
		}
		try {
			if (mode == Mode.WRITE) {
				this.writer = new BufferedOutputStream(new DataOutputStream(new FileOutputStream(path)));
				this.reader = null;
			} else {
				this.reader = new BufferedInputStream(new DataInputStream(new FileInputStream(path)));
				this.writer = null;
			}
		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException("Could not open " + path.getAbsolutePath());
		}
		this.mode = mode;
	}
	
	public void writeByteString(ByteString key) throws IOException {
		if (mode == Mode.READ) {
			throw new IllegalArgumentException ("KeyJournal is in READ mode for write operation");
		}
		if (key == null) {
			throw new IllegalArgumentException("key must not be null");
		}
		byte keyBytes[] = key.toByteArray();
		this.writer.write(keyBytes.length);
		this.writer.write(keyBytes, 0, keyBytes.length);
	}
	
	public void write(String bucket, String key) throws IOException {
		if (mode == Mode.READ) {
			throw new IllegalArgumentException ("KeyJournal is in READ mode for write operation");
		}
		if (bucket == null || key == null) {
			throw new IllegalArgumentException("bucket and key must not be null");
		}
		//this.writer.write((bucket + "," + key + "\n"));
	}
	
	public void write(RiakObject riakObject) throws IOException {
		this.write(riakObject.getBucket(), riakObject.getKey());
	}
	
	public void write(IRiakObject riakObject) throws IOException {
		this.write(riakObject.getBucket(), riakObject.getKey());
	}
	
	public ByteString readByteString() throws IOException {
		int len = this.reader.read();
		if (len == -1) { // EOF
			return null;
		}
		byte[] key = new byte[len];
		this.reader.read(key, 0, len);
		return(ByteString.copyFrom(key));
	}

	public Key read() throws IOException {
		if (mode == Mode.WRITE) {
			throw new IllegalArgumentException("KeyJournal is in WRITE mode for read operation");
		}
		//String line = this.reader.readLine();
		String line = null;
		if (line == null) {
			return null;
		}
		String[] values = new String[2];
		int comma = line.indexOf(',');
		if (comma != -1) {
			values[0] = line.substring(0, comma);
			values[1] = line.substring(comma + 1, line.length());
			return new Key(values[0], values[1]);
		}
		return null;
	}
		
	public void close() {
		try {
			if (this.writer != null) {
				this.writer.flush();
				this.writer.close();
			}
			if (this.reader != null) {
				this.reader.close();
			}
		} catch (IOException e) {
			// no-op, swallow
		}
		this.closed = true;
	}
	public boolean isClosed() {
		return this.closed;
	}

	@Override
	public Iterator<Key> iterator() {
		return new KeyIterator(this);
	}
	
	public static File createKeyPathFromPath(File file, boolean load) {
		String path = file.getAbsolutePath(); 
		int ind = path.lastIndexOf('.');
		if (ind == -1) {
			ind = path.length()-1;
		}
		path = path.substring(0, ind);
		if (load) {
			path = path + ".loadedkeys";
		} else {
			path = path + ".keys";
		}
		return new File(path);
	}
	
	private class KeyIterator implements Iterator<Key> {
		private final KeyJournal keyJournal;
		
		private Key nextKey;
		
		public KeyIterator(KeyJournal keyJournal) {
			this.keyJournal = keyJournal;
			try {
				this.nextKey = keyJournal.read();
			} catch (IOException e) {
				this.nextKey = null;
			}
		}
		
		@Override
		public boolean hasNext() {
			return this.nextKey != null;
		}

		@Override
		public Key next() {
			Key currentKey = this.nextKey;
			try {
				this.nextKey = this.keyJournal.read();
			} catch (IOException e) {
				this.nextKey = null;
			}
			if (currentKey == null && this.nextKey == null) {
				currentKey = Key.createErrorKey();
			}
			return currentKey;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
	
}
