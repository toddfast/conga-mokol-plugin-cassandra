package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.Shell.CommandContext;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.Command;
import com.conga.tools.mokol.Usage;
import java.lang.reflect.Method;

/**
 *
 * @author Todd Fast
 */
public abstract class AbstractCQLCommand extends Command {

	/**
	 *
	 *
	 */
	protected static CQLLoader getLoader(CommandContext context)
			throws ShellException {
		return context.getShell().getEnvironmentValue(ENV_CQL_LOADER_INSTANCE,
			CQLLoader.class);
	}


	/**
	 *
	 *
	 */
	protected static void updateStepEnvironment(CommandContext context)
			throws ShellException {
		CQLLoader loader=getLoader(context);
		if (loader!=null) {
			context.getShell().getEnvironment().put(
				ENV_STEP,loader.getCurrentStep());
		}
	}


	/**
	 *
	 *
	 * @param context
	 * @return
	 */
	@Override
	public Usage getUsage(CommandContext context) {
		return super.getUsage(context);
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	protected static final String ENV_CQL_LOADER_INSTANCE=
		CQLLoader.class.getName()+".instance";
	protected static final String ENV_STEP="step";
}
