package com.basho.proserv.datamigrator.riak;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.io.Key;
import com.basho.proserv.datamigrator.io.SyncedKeyJournal;
import com.basho.proserv.datamigrator.riak.Connection;
import com.basho.proserv.datamigrator.util.NamedThreadFactory;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.pbc.ConversionUtilWrapper;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class ThreadedMirror {
	private final Logger log = LoggerFactory.getLogger(ThreadedMirror.class);
	private final Connection readConnections[];
	private final Connection writeConnections[];
	private final SyncedKeyJournal keys;
	private final SyncedKeyJournal failedKeys;
	private final int workerCount;
	private int finishedWorkers;
	private int processedCount;

	private final NamedThreadFactory threadFactory = new NamedThreadFactory();
	private final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	private final List<Future<Runnable>> threads = new ArrayList<Future<Runnable>>();

	private static final int MAX_TRIES = 3;
	
	public ThreadedMirror(Connection readConnections[], Connection writeConnections[],
				SyncedKeyJournal keys, SyncedKeyJournal failedKeys, int workerCount) {
		this.readConnections = readConnections;
		this.writeConnections = writeConnections;
		this.keys = keys;
		this.failedKeys = failedKeys;
		this.workerCount = workerCount;
		this.finishedWorkers = 0;
		this.processedCount = 0;
		
		this.run();
	}
	
	public synchronized boolean finished() {
		return this.finishedWorkers == this.workerCount;
	}

	public synchronized int numberProcessed() {
		return this.processedCount;
	}

	public void close() {
		this.executor.shutdown();
	}

	private synchronized void workerFinished(int processedCount) {
		this.finishedWorkers++;
		this.processedCount += processedCount;
	}

	private void interruptWorkers() {
		for (Future<Runnable> future : this.threads) {
			future.cancel(true);
		}
	}
	
	
	@SuppressWarnings("unchecked")
	private void run() {
		for (Integer i = 0; i < this.workerCount; ++i) {
			this.threadFactory.setNextThreadName(String.format("MirrorThread-%d", i));
			this.threads.add((Future<Runnable>) executor.submit(new MirrorThread(this, this.readConnections[i],
					new MirrorWriter(this.writeConnections[i]), this.keys, this.failedKeys)));
		}
	}
	
	private class MirrorThread implements Runnable {
		private final ThreadedMirror threadedMirror;
		private final Connection readConnection;
		private final IClientWriter writer;
		private final SyncedKeyJournal keys;
		private final SyncedKeyJournal failedKeys;
		
		public MirrorThread(ThreadedMirror threadedMirror, Connection readConnection,
					IClientWriter writer, SyncedKeyJournal keys, SyncedKeyJournal failedKeys) {
			this.threadedMirror = threadedMirror;
			this.readConnection = readConnection;
			this.writer = writer;
			this.keys = keys;
			this.failedKeys = failedKeys;
		}

		@Override
		public void run() {
			int processedCount = 0;
			try {
				while (!Thread.interrupted()) {
					ByteString key = this.keys.read();
					if (key == null) {
						log.error("Nothing left to read, shutting down thread");
						break;
					}

					processedCount++;

					//System.out.println("we got here, b="+ key.bucket() +", k="+ key.key());
					try {
						IRiakObject[] objects = readObject(keys.getBucketName(), key);

						if (objects != null && objects.length > 0) {
						  storeObject(objects[0]);
						} else {
							failedKeys.write(key);
							logKey("FTFO", key);
						}
					} catch (Exception exc) {
						exc.printStackTrace(System.out);
						failedKeys.write(key);
						logKey("FTWO", key);
					}
				}
			} catch (Exception e) {
				log.info("Interrupted, shutting down thread ");
				//no-op
			} finally {
				threadedMirror.workerFinished(processedCount);
			}
		}

		private IRiakObject[] readObject(ByteString bucketName, ByteString key) {
			IRiakObject[] objects = null;
			for (int i=0; objects == null && i < MAX_TRIES; i++) {
				try {
					RiakResponse resp = ((PBClientAdapter)this.readConnection.riakClient).fetch(bucketName, key);
					objects = resp.getRiakObjects();
				} catch (Exception e) {
					//e.printStackTrace();
					// ignore..probably connection broked, allow it to reconnect
				}
			}
			return objects;
		}

		private void storeObject(IRiakObject object) {
			for (int i=0; i < MAX_TRIES; i++) {
				try {
					 this.writer.storeRiakObject(object);
					 return;
				} catch (Exception e) {
					// ignore..probably connection broked, allow it to reconnect
				}
			}
		}

		private void logKey(String txt, ByteString key) {
			try {
				log.error(txt + key.toString());
			} catch (Exception exc) {
				log.error("FTWO="+ exc.getMessage());
			}
		}
	}
}
