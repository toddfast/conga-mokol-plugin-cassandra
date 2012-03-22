package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.CommandContext;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.plugin.cassandra.ConnectionException;
import com.conga.tools.mokol.spi.Command;

/**
 *
 * 
 * @author Todd Fast
 */
public abstract class AbstractCQLCommand extends Command {

	/**
	 *
	 *
	 */
	protected CQLLoader getLoader(CommandContext context)
			throws ShellException {

		return getLoader(context,true);
	}


	/**
	 *
	 *
	 */
	protected CQLLoader getLoader(CommandContext context,
			boolean checkConnection)
			throws ShellException {

		CQLLoader result=context.getShell().getEnvironment().get(
			ENV_CQL_LOADER_INSTANCE,CQLLoader.class);

		if (checkConnection && result==null) {
			throw new ConnectionException("Not connected");
		}

		return result;
	}


	/**
	 *
	 *
	 */
	protected void updateStepEnvironment(CommandContext context)
			throws ShellException {

		CQLLoader loader=getLoader(context);
		if (loader!=null) {
			context.getShell().getEnvironment().put(
				ENV_STEP,loader.getCurrentStep());
		}
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	protected static final String ENV_CQL_LOADER_INSTANCE=
		CQLLoader.class.getName()+".instance";
	protected static final String ENV_STEP="step";
}
