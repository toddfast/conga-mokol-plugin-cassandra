package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.Shell;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.spi.CommandClassFactory;
import com.conga.tools.mokol.spi.Plugin;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author Todd Fast
 */
public class CQLPlugin implements Plugin {

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
	/*pkg*/ Map<String,Object> getEnvironment() {
		return Collections.unmodifiableMap(_getEnvironment());
	}


	/**
	 *
	 *
	 */
	private Map<String,Object> _getEnvironment() {
		return environment;
	}


	/**
	 *
	 *
	 */
	/*pkg*/ <T> T getEnvironmentValue(String key, Class<T> clazz) {
		return clazz.cast(_getEnvironment().get(key));
	}


	/**
	 *
	 *
	 */
	/*pkg*/ void putEnvironmentValue(String key, Object value) {
		if (key==null || key.trim().isEmpty()) {
			throw new IllegalArgumentException("Parameter \"key\" cannot be "+
				"null or an empty string");
		}

		_getEnvironment().put(key,value);
	}


	/**
	 *
	 *
	 */
	/*pkg*/ void removeEnvironmentValue(String key) {
		if (key==null || key.trim().isEmpty()) {
			throw new IllegalArgumentException("Parameter \"key\" cannot be "+
				"null or an empty string");
		}

		_getEnvironment().remove(key);
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
