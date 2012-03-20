package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.Shell;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.CommandClassFactory;
import com.conga.tools.mokol.plugin.cassandra.cql.AbstractCQLCommand;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * @author Todd Fast
 */
public class ConnectCommand extends AbstractCQLCommand {

	/**
	 * 
	 * 
	 */
	@Override
	public void execute(Shell.CommandContext context, List<String> args)
			throws ShellException {

		try {
			CQLLoader loader=getLoader(context);
			if (loader!=null) {
				context.getShell().executeCommand("disconnect",
					new CommandClassFactory(DisconnectCommand.class),null);
			}

			String url=args.get(0);
			if (!url.startsWith("jdbc")) {
				String newURL="jdbc:cassandra:";
				if (!url.contains("@")) {
					newURL+="user/password@";
				}

				String suffix="";
				if (!url.contains("/") ||
						url.substring(url.lastIndexOf("/")).length() <= 1) {
					throw new ShellException("Invalid JDBC URL. The Cassandra "+
						"JDBC URL format requires at minimum "+
						"\"server:port/keyspace\"");
				}
				
				newURL+=url+suffix;
				url=newURL;
			}

//			String resourceRoot=args.get(1);

			// Build the loader
			loader=CQLLoader.url(url)
//				.resourceRoot(resourceRoot)
				.skipExecution(true)
				.build();

			// Open the connection
			loader.openConnection();

			// Remember this in the environment
//			context.getShell().getEnvironment().put(
//				Environment.ENV_RESOURCE_ROOT,resourceRoot);
			context.getShell().getEnvironment().put(
				ENV_CQL_LOADER_INSTANCE,loader);

			updateStepEnvironment(context);

			context.getShell().pushPromptFormat(PROMPT);

//			context.printf("Warning: skip execution is set to %s. "+
//				"Use \"skip %s\" to toggle.\n",
//				getLoader(context).getSkipExecution() ? "on" : "off",
//				!getLoader(context).getSkipExecution() ? "off" : "on");
		}
		catch (SQLException e) {
			throw new ShellException("Exception trying to connect",e);
		}
		catch (IOException e) {
			throw new ShellException("Could not load CQL resources",e);
		}
		catch (IndexOutOfBoundsException e) {
			wrongNumberOfParameters(2,2,args.size());
		}
	}


	/**
	 *
	 * 
	 */
	public String getUsage() {
		return "Connect to a Cassandra node. "+
			"Usage: connect [[jdbc:cassandra:]<user>/<password>@]<server>:<port>/<keyspace>";
	}



	private static final String PROMPT="[{"+ENV_CQL_LOADER_INSTANCE+"%s}] ";
}
