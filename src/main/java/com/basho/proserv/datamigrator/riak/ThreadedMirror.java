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
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class ThreadedMirror {
	private final Logger log = LoggerFactory.getLogger(ThreadedMirror.class);
	private final Connection readConnections[];
	private final Connection writeConnections[];
	private final SyncedKeyJournal keys;
	private final int workerCount;

	private final NamedThreadFactory threadFactory = new NamedThreadFactory();
	private final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	private final List<Future<Runnable>> threads = new ArrayList<Future<Runnable>>();

	private static final int MAX_TRIES = 3;
	
	public ThreadedMirror(Connection readConnections[], Connection writeConnections[], SyncedKeyJournal keys, int workerCount) {
		this.readConnections = readConnections;
		this.writeConnections = writeConnections;
		this.keys = keys;
		this.workerCount = workerCount;
		
		this.run();
	}
	
	public void close() {
		this.executor.shutdown();
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
			this.threads.add((Future<Runnable>) executor.submit(new MirrorThread(this.readConnections[i],
					new MirrorWriter(this.writeConnections[i]), this.keys)));
		}
	}
	
	private class MirrorThread implements Runnable {
		private final Connection readConnection;
		private final IClientWriter writer;
		private final SyncedKeyJournal keys;
		
		public MirrorThread(Connection readConnection, IClientWriter writer, SyncedKeyJournal keys) {
			this.readConnection = readConnection;
			this.writer = writer;
			this.keys = keys;
		}

		@Override
		public void run() {
			try {
				while (!Thread.interrupted()) {
					Key key = this.keys.read();
					if (key == null) {
						log.error("Nothing left to read, shutting down thread "+ Thread.currentThread().getName());
						break;
					}

					//System.out.println("we got here, b="+ key.bucket() +", k="+ key.key() +", tid="+ Thread.currentThread().getName());
					try {
						IRiakObject[] objects = readObject(key);

						if (objects.length > 0) {
						  storeObject(objects[0]);
						} else {
							log.error("FTFO="+ key.bucket() +","+ key.key());
						}
					} catch (Exception exc) {
						exc.printStackTrace(System.out);
						log.error("FTWO="+ key.bucket() +","+ key.key());
					}
				}
			} catch (Exception e) {
				log.info("Interrupted, shutting down thread "+ Thread.currentThread().getName());
				//no-op
			}
		}

		private IRiakObject[] readObject(Key key) {
			IRiakObject[] objects = null;
			for (int i=0; objects == null && i < MAX_TRIES; i++) {
				try {
					RiakResponse resp = this.readConnection.riakClient.fetch(key.bucket(), key.key());
					objects = resp.getRiakObjects();
				} catch (Exception e) {
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
	}
}
