package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.spi.AnnotatedCommand;
import com.conga.tools.mokol.spi.CommandContext;
import com.conga.tools.mokol.spi.Usage;

/**
 *
 * @author Todd Fast
 */
public abstract class AbstractCQLCommand extends AnnotatedCommand {

	/**
	 *
	 *
	 */
	protected CQLLoader getLoader(CommandContext context)
			throws ShellException {
		return ((CQLPlugin)getPlugin(context)).getEnvironmentValue(
			ENV_CQL_LOADER_INSTANCE,CQLLoader.class);
	}


	/**
	 *
	 *
	 */
	protected void updateStepEnvironment(CommandContext context)
			throws ShellException {
		CQLLoader loader=getLoader(context);
		if (loader!=null) {
			((CQLPlugin)getPlugin(context)).putEnvironmentValue(
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
