package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.Shell;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.spi.AbstractPlugin;
import com.conga.tools.mokol.spi.CommandClassFactory;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author Todd Fast
 */
public class CQLPlugin extends AbstractPlugin {

	/**
	 *
	 *
	 */
	@Override
	public String getName() {
		return "Cassandra CQL Tools";
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
	 */
	@Override
	protected Map<String,Object> getEnvironment() {
		return super.getEnvironment();
	}


	/**
	 *
	 *
	 */
	@Override
	protected <T> T getEnvironmentValue(String key, Class<T> clazz) {
		return super.getEnvironmentValue(key,clazz);
	}


	/**
	 *
	 *
	 */
	@Override
	protected void putEnvironmentValue(String key, Object value) {
		super.putEnvironmentValue(key,value);
	}


	/**
	 *
	 *
	 */
	@Override
	protected void removeEnvironmentValue(String key) {
		super.removeEnvironmentValue(key);
	}


	/**
	 *
	 *
	 */
	@Override
	public void initialize(Shell shell) throws ShellException {
		shell.aliasCommand("cql",this,
			new CommandClassFactory(ExecuteCQLCommand.class));
		shell.aliasCommand("select",this,
			new CommandClassFactory(SelectCommand.class));

		shell.aliasCommand("update",this,
			new CQLUpdateCommand.Factory(CQLUpdateCommand.Verb.UPDATE));
		shell.aliasCommand("delete",this,
			new CQLUpdateCommand.Factory(CQLUpdateCommand.Verb.DELETE));

		shell.aliasCommand("count",this,
			new CommandClassFactory(CountCommand.class));

		shell.aliasCommand("export",this,
			new CommandClassFactory(ExportCommand.class));
		shell.aliasCommand("dump",this,
			new CommandClassFactory(DumpCommand.class));
	}



	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private Map<String,Object> environment=Collections.synchronizedMap(
		new TreeMap<String,Object>());
}
