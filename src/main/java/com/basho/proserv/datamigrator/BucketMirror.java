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

import com.google.protobuf.ByteString;

// BucketMirror will only work with clients returning protobuffer objects, ie PBClient
public class BucketMirror {
	private final Logger log = LoggerFactory.getLogger(BucketMirror.class);
	public final Summary summary = new Summary();
	private final Connection readConnections[];
	private final Connection writeConnections[];
	private final File dataRoot;
	private final boolean verboseStatusOutput;
	private final int riakWorkerCount;
	private int errorCount = 0;
	
	private long timerStart = System.currentTimeMillis();
	private long previousCount = 0;
	
	public BucketMirror(Connection readConnections[], Connection writeConnections[], File dataRoot, 
			boolean verboseStatusOutput, int riakWorkerCount) {
		
		this.readConnections = readConnections;
		this.writeConnections = writeConnections;
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
		
		
		if (!this.writeConnections[0].connected()) {
			log.error("Not write connected to Riak");
			return 0;
		}
		
		if (this.verboseStatusOutput) {
			System.out.println("\nDumping bucket " + bucketName);
		}
		
		File keyPath = new File(this.createBucketPath(bucketName) + "/bucketkeys.keys");
		File failPath = new File(this.createBucketPath(bucketName) + "/bucketkeys.failed");
		if (!keyPath.exists()) {
			log.error("No key file to read from");
			System.out.println("\nNo key file to read from");
			return 0;
		}
		
		// Get input KeyJournal
		KeyJournal bucketKeys = new KeyJournal(keyPath, KeyJournal.Mode.READ);
		//long key_count = countAllKeys(bucketKeys);
				
		// Place to store keys that fail to read/write
		KeyJournal failJournal = new KeyJournal(failPath, KeyJournal.Mode.WRITE);

		// Create wrapper around KeyJournal
		SyncedKeyJournal keys = new SyncedKeyJournal(bucketName, bucketKeys);
		SyncedKeyJournal failedKeys = new SyncedKeyJournal(bucketName, failJournal);

		// Spin up X threads to read from old then write to new
		ThreadedMirror mirroror = new ThreadedMirror(readConnections, writeConnections, keys, failedKeys, this.riakWorkerCount);
		try {
			while (!mirroror.finished()) {
				Thread.sleep(500);
			}
		} catch (Exception e) {
			log.error("Riak error dumping objects for bucket: " + bucketName, e);
			e.printStackTrace();
		} finally {
			bucketKeys.close();
			failJournal.close();
			mirroror.close();
		}
		
		long stop = System.currentTimeMillis();
		
		System.out.println("number failures: "+ failedKeys.numberProcessed());
		printStatus(mirroror.numberProcessed(), mirroror.numberProcessed(), true);
		return mirroror.numberProcessed();
	}
	
	public void splitKeys(Set<String> bucketNames, int chunkSize) {
		for (String bucketName : bucketNames) {
			splitKeys(bucketName, chunkSize);
		}
	}

	public void splitKeys(String bucketName, int chunkSize) {
		File keyPath = new File(this.createBucketPath(bucketName) + "/bucketkeys.keys");
		if (!keyPath.exists()) {
			log.error("No key file to read from");
			System.out.println("\nNo key file to read from");
			return;
		}
		
		KeyJournal reader = new KeyJournal(keyPath, KeyJournal.Mode.READ);
		KeyJournal writer = null;
		long fileCount = 0;
		ByteString key = null;
		try {
			for (long count=0; (key=reader.readByteString()) != null; count++) {
				if ((count%chunkSize) == 0) {
					if (writer != null) {
						writer.close();
					}
					File writePath = new File(this.createBucketPath(bucketName) + "/bucketkeys."+ fileCount++);
		    	writer = new KeyJournal(writePath, KeyJournal.Mode.WRITE);
				}

				writer.writeByteString(key);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (writer != null) {
			writer.close();
		}
		reader.close();
	}

	public int errorCount() {
		return errorCount;
	}

	private String createBucketPath(String bucketName) {
		String encodedBucketName = Utilities.urlEncode(bucketName);
		return this.dataRoot.getAbsolutePath() + "/" + encodedBucketName;
	}

	private long countAllKeys(KeyJournal keys) {
		long keyCount = 0;
		try {
			while (keys.readByteString() != null) {
				keyCount++;
			}
		} catch (IOException ioe) {
			System.out.println("XX read before die: There are "+ keyCount +" keys");
			ioe.printStackTrace();
		}

		return keyCount;
	}

	private void printStatus(long keyCount, long objectCount, boolean force) {
		long end = System.currentTimeMillis();
		if (end-timerStart >= 1000 || force) {
			long total = end-timerStart;
			int recsSec = (int)((objectCount-this.previousCount)/(total/1000.0));
			int perc = (int)((double)objectCount/(double)keyCount * 100);
			String msg = String.format("\r%d%% completed. Read %d @ %d obj/sec          \n", perc, objectCount, recsSec);
			System.out.print(msg);
			System.out.flush();
			
			this.previousCount = objectCount;
			timerStart = System.currentTimeMillis();
		}
	}
}
