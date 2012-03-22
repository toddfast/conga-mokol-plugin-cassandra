package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.CommandContext;
import com.conga.tools.mokol.plugin.cassandra.CassandraPluginBase;
import com.conga.tools.mokol.spi.annotation.Command;
import com.conga.tools.mokol.spi.annotation.Plugin;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * 
 * @author Todd Fast
 */
@Plugin(
	commands={
		@Command(alias="cql", command=ExecuteCQLCommand.class),
		@Command(alias="select", command=SelectCommand.class),
		@Command(alias="count", command=CountCommand.class),
		@Command(alias="export", command=ExportCommand.class),
		@Command(alias="dump", command=DumpCommand.class)
	})
public class CQLPlugin extends CassandraPluginBase {

	/**
	 *
	 *
	 */
	@Override
	public String getName() {
		return "Cassandra CQL Tools";
	}


//	/**
//	 *
//	 *
//	 */
//	@Override
//	protected Map<String,Object> getEnvironment() {
//		return super.getEnvironment();
//	}
//
//
//	/**
//	 *
//	 *
//	 */
//	@Override
//	protected <T> T getEnvironmentValue(String key, Class<T> clazz) {
//		return super.getEnvironmentValue(key,clazz);
//	}
//
//
//	/**
//	 *
//	 *
//	 */
//	@Override
//	protected void putEnvironmentValue(String key, Object value) {
//		super.putEnvironmentValue(key,value);
//	}
//
//
//	/**
//	 *
//	 *
//	 */
//	@Override
//	protected void removeEnvironmentValue(String key) {
//		super.removeEnvironmentValue(key);
//	}


	/**
	 *
	 *
	 */
	@Command(alias="update")
	public CQLUpdateCommand update(CommandContext context) {
		return new CQLUpdateCommand(CQLUpdateCommand.Verb.UPDATE);
	}


	/**
	 *
	 *
	 */
	@Command(alias="delete")
	public CQLUpdateCommand delete(CommandContext context) {
		return new CQLUpdateCommand(CQLUpdateCommand.Verb.DELETE);
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private Map<String,Object> environment=Collections.synchronizedMap(
		new TreeMap<String,Object>());
}
