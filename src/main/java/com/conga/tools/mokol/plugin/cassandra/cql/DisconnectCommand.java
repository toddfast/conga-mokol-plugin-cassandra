package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.Shell;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.plugin.cassandra.CassandraEnvironment;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * @author Todd Fast
 */
public class DisconnectCommand extends AbstractCQLCommand {

	/**
	 *
	 * 
	 */
	@Override
	public void execute(Shell.CommandContext context,List<String> args)
			throws ShellException {
		try {
			CQLLoader loader=getLoader(context);
			if (loader!=null) {
				loader.closeConnection();

				context.getShell().getEnvironment().remove(
					ENV_CQL_LOADER_INSTANCE);
				context.getShell().getEnvironment().remove(
					ENV_STEP);
				context.getShell().getEnvironment().remove(
					CassandraEnvironment.ENV_RESOURCE_ROOT);

				context.getShell().popPromptFormat();
			}
		}
		catch (SQLException e) {
			throw new ShellException("Exception trying to disconnect",e);
		}
	}


	/**
	 *
	 * 
	 */
	public String getUsage() {
		return "Disconnect from the current Cassandra instance";
	}
}
