package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.client.raw.MatchFoundException;

public class MirrorWriter implements IClientWriter {

	private final Connection connection;
	
	public MirrorWriter(Connection connection) {
		this.connection = connection;
	}
	
	@Override
	public IRiakObject storeRiakObject(IRiakObject riakObject) throws IOException {
		StoreMeta storeMeta = new StoreMeta.Builder().ifNoneMatch(true).build();
		try {
			this.connection.riakClient.store(riakObject, storeMeta);
		} catch (MatchFoundException mfe) {
			// swallow the match found exception
		}
		return riakObject;
	}

}
