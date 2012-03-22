package com.conga.tools.mokol.plugin.cassandra;

import com.conga.tools.mokol.spi.Plugin;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * 
 * @author Todd Fast
 */
public abstract class CassandraPluginBase extends Plugin {

	/**
	 * 
	 * 
	 */
	public CassandraPluginBase() {
		super();

		instances.put(getClass().getName(),this);
	}


	/**
	 *
	 *
	 */
	@Override
	public String getVersion() {
		return getClass().getPackage().getImplementationVersion();
	}


	/**
	 *
	 *
	 * @return
	 */
	protected Map<String,Object> getMutableEnvironment() {
		return environment;
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private static Map<String,CassandraPluginBase> instances=
		new HashMap<String,CassandraPluginBase>();
	private static Map<String,Object> environment=Collections.synchronizedMap(
		new TreeMap<String,Object>());
}
