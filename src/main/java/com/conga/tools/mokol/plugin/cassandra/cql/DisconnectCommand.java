package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.plugin.cassandra.CassandraEnvironment;
import com.conga.tools.mokol.spi.CommandContext;
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
	public void doExecute(CommandContext context,List<String> args)
			throws ShellException {
		try {
			CQLLoader loader=getLoader(context);
			if (loader!=null) {
				loader.closeConnection();

				CQLPlugin plugin=((CQLPlugin)getPlugin(context));
				plugin.removeEnvironmentValue(
					ENV_CQL_LOADER_INSTANCE);
				plugin.removeEnvironmentValue(
					ENV_STEP);
				plugin.removeEnvironmentValue(
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
