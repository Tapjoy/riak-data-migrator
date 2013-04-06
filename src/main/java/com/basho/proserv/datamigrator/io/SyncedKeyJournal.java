package com.basho.proserv.datamigrator.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import com.google.protobuf.ByteString;

public class SyncedKeyJournal {
	private final Logger log = LoggerFactory.getLogger(SyncedKeyJournal.class);
	private KeyJournal keys;
	private ByteString bucketName;
	private boolean finished;
	private long count;
	private static long LOG_MARKER = 100000;

	public SyncedKeyJournal(String bucketName, KeyJournal keys) {
		this.keys = keys;
		this.finished = false;
		this.count = 0;
		this.bucketName = ByteString.copyFromUtf8(bucketName);
	}

	// probably safe for this not to be synced, it's only set once during init
	public ByteString getBucketName() {
		return this.bucketName;
	}

	public synchronized void write(ByteString keyString) throws IOException {
		keys.writeByteString(keyString);
		count++;
	}

	public synchronized ByteString read() throws IOException {
		if (this.finished) {
			return null;
		}

    ByteString key = this.keys.readByteString();
		if (key == null) {
			this.finished = true;
		}

		logKey(key);
		return key;
	}

	public synchronized long numberProcessed() {
		return this.count;
	}

	private void logKey(ByteString key) {
		if ((++this.count % LOG_MARKER) == 0) {
			try {
			log.error("Read["+ this.count +"] = "+ key);
			} catch (Exception e) {
				// could we die printing the key to stdout if it's not utf8?
			  //log.error("Read["+ count +"] ..writing key failed..possibly not utf8? ("+ key.isValidUtf8() +")");
			  log.error("Read["+ this.count +"] ..writing key failed..possibly not utf8?");
			}
		}
	}
}
