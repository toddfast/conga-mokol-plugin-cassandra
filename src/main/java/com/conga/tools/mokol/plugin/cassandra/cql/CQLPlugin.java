package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.CommandClassFactory;
import com.conga.tools.mokol.Plugin;
import com.conga.tools.mokol.Shell;
import com.conga.tools.mokol.ShellException;

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
		return getClass().getName()
			.substring(0,getClass().getName().lastIndexOf("."));
	}


	/**
	 *
	 *
	 */
	@Override
	public String getVersion() {
		return "0.1";
	}


	/**
	 *
	 *
	 */
	@Override
	public void initialize(Shell shell) throws ShellException {
		shell.aliasCommand("cql",
			new CommandClassFactory(ExecuteCQLCommand.class));
		shell.aliasCommand("select",
			new CommandClassFactory(SelectCommand.class));
		shell.aliasCommand("update",new CQLUpdateCommand.Factory(
			CQLUpdateCommand.Verb.UPDATE));
		shell.aliasCommand("delete",new CQLUpdateCommand.Factory(
			CQLUpdateCommand.Verb.DELETE));

		shell.aliasCommand("count",new CommandClassFactory(CountCommand.class));

		shell.aliasCommand("export",
			new CommandClassFactory(ExportCommand.class));
		shell.aliasCommand("dump",new CommandClassFactory(DumpCommand.class));
	}
}
