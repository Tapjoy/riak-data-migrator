package com.basho.proserv.datamigrator.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SyncedKeyJournal {
	private final Logger log = LoggerFactory.getLogger(SyncedKeyJournal.class);
	private KeyJournal keys;
	private boolean finished;
	private long count;
	private static long LOG_MARKER = 100000;

	public SyncedKeyJournal(KeyJournal keys) {
		this.keys = keys;
		this.finished = false;
		this.count = 0;
	}

	public synchronized Key read() throws IOException {
    Key key = this.keys.read();
		if (key == null) {
			this.finished = true;
		}
		if ((++count % LOG_MARKER) == 0) {
			log.error("Read["+ count +"] = "+ (key == null ? "" : key.key()));
		}
		return key;
	}

	public synchronized boolean finished() {
		return this.finished;
	}
}
