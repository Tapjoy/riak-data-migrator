package com.basho.proserv.datamigrator;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.io.KeyJournal;
import com.basho.proserv.datamigrator.io.SyncedKeyJournal;
import com.basho.proserv.datamigrator.riak.Connection;
import com.basho.proserv.datamigrator.riak.ThreadedMirror;

// BucketMirror will only work with clients returning protobuffer objects, ie PBClient
public class BucketMirror {
	private final Logger log = LoggerFactory.getLogger(BucketMirror.class);
	public final Summary summary = new Summary();
	private final Connection readConnection;
	private final Connection writeConnection;
	private final File dataRoot;
	private final boolean verboseStatusOutput;
	private final int riakWorkerCount;
	private int errorCount = 0;
	
	private long timerStart = System.currentTimeMillis();
	private long previousCount = 0;
	
	public BucketMirror(Connection readConnection, Connection writeConnection, File dataRoot, 
			boolean verboseStatusOutput, int riakWorkerCount) {
		
		this.readConnection = readConnection;
		this.writeConnection = writeConnection;
		this.dataRoot = dataRoot;
		this.verboseStatusOutput = verboseStatusOutput;
		this.riakWorkerCount = riakWorkerCount;
	}
	
	public long mirrorBuckets(Set<String> bucketNames) {
		int objectCount = 0;
		for (String bucketName : bucketNames) {
			objectCount += mirrorBucket(bucketName);
		}
		return objectCount;
	}
	
	public long mirrorBucket(String bucketName) {
		if (bucketName == null || bucketName.isEmpty()) {
			throw new IllegalArgumentException("bucketName cannot be null or empty");
		}

		long start = System.currentTimeMillis();
		
		if (!this.readConnection.connected()) {
			log.error("Not read connected to Riak");
			return 0;
		}
		
		if (!this.writeConnection.connected()) {
			log.error("Not write connected to Riak");
			return 0;
		}
		
		if (this.verboseStatusOutput) {
			System.out.println("\nDumping bucket " + bucketName);
		}
		
		File keyPath = new File(this.createBucketPath(bucketName) + "/bucketkeys.keys");
		if (!keyPath.exists()) {
			log.error("No key file to read from");
			System.out.println("\nNo key file to read from");
			return 0;
		}
		
		// Get input KeyJournal
		KeyJournal bucketKeys = new KeyJournal(keyPath, KeyJournal.Mode.READ);
				
		// Create wrapper around KeyJournal
		SyncedKeyJournal keys = new SyncedKeyJournal(bucketKeys);

		// Spin up X threads to read from old then write to new
		ThreadedMirror mirroror = new ThreadedMirror(readConnection, writeConnection, keys, this.riakWorkerCount);
		try {
			while (!keys.finished()) {
				Thread.sleep(500);
			}
		} catch (Exception e) {
			log.error("Riak error dumping objects for bucket: " + bucketName, e);
			e.printStackTrace();
		} finally {
			bucketKeys.close();
			mirroror.close();
		}
		
		long stop = System.currentTimeMillis();
		
		return 42;
	}
	
	public int errorCount() {
		return errorCount;
	}

	private String createBucketPath(String bucketName) {
		String encodedBucketName = Utilities.urlEncode(bucketName);
		return this.dataRoot.getAbsolutePath() + "/" + encodedBucketName;
	}
}
