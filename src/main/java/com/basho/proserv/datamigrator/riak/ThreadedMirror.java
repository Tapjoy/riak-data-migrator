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
	private final Connection readConnection;
	private final Connection writeConnection;
	private final SyncedKeyJournal keys;

	
	private final NamedThreadFactory threadFactory = new NamedThreadFactory();
	private final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	private final int workerCount;

	private final List<Future<Runnable>> threads = new ArrayList<Future<Runnable>>();
	
	private int stoppedCount = 0;
	
	public ThreadedMirror(Connection readConnection, Connection writeConnection, SyncedKeyJournal keys, int workerCount) {
		this.readConnection = readConnection;
		this.writeConnection = writeConnection;
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
			this.threads.add((Future<Runnable>) executor.submit(new MirrorThread(this.readConnection,
					new MirrorWriter(this.writeConnection), this.keys)));
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
						log.info("Nothing left to read, shutting down thread "+ Thread.currentThread().getName());
						break;
					}

					try {
						// MORE WORK HERE
						// This exchange has not been flushed out. vclock? Prob ok since only create if not match.
						RiakResponse resp = this.readConnection.riakClient.fetch(key.bucket(), key.key());
						IRiakObject[] objects = resp.getRiakObjects();
						if (objects.length > 0) {
						  this.writer.storeRiakObject(objects[0]);
						} else {
							log.error("Failed to find object, b="+ key.bucket() +", k="+ key.key());
						}
					} catch (Exception exc) {
						log.error("Failed to write, b="+ key.bucket() +", k="+ key.key() +", msg="+ exc.getMessage());
					}
				}
			} catch (Exception e) {
				log.info("Interrupted, shutting down thread "+ Thread.currentThread().getName());
				//no-op
			}
		}
	}
}
