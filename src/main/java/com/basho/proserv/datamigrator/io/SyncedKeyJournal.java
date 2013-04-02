package com.basho.proserv.datamigrator.io;

import java.io.IOException;

public class SyncedKeyJournal {
	private KeyJournal keys;
	private boolean finished;

	public SyncedKeyJournal(KeyJournal keys) {
		this.keys = keys;
		this.finished = false;
	}

	public synchronized Key read() throws IOException {
    Key key = this.keys.read();
		if (key == null) {
			this.finished = true;
		}
		return key;
	}

	public synchronized boolean finished() {
		return this.finished;
	}
}
