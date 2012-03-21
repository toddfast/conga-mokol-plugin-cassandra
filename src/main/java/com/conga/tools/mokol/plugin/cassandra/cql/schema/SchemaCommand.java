package com.conga.tools.mokol.plugin.cassandra.cql.schema;

import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.plugin.cassandra.cql.AbstractCQLCommand;
import com.conga.tools.mokol.plugin.cassandra.cql.CQLLoader;
import com.conga.tools.mokol.plugin.cassandra.CassandraEnvironment;
import com.conga.tools.mokol.spi.CommandContext;
import com.conga.tools.mokol.spi.annotation.Example;
import com.conga.tools.mokol.spi.annotation.Help;
import java.io.IOException;
import java.util.List;

/**
 *
 * 
 * @author Todd Fast
 */
@Help(
	value="Load a CQL schema",
	examples={
		@Example(
			value="org/mystuff/cql",
			description="Load the schema from the specified resource root"
		)
	}
)
public class SchemaCommand extends AbstractCQLCommand {

	/**
	 * 
	 * 
	 */
	@Override
	public void doExecute(CommandContext context, List<String> args)
			throws ShellException {

		try {
			CQLLoader loader=getLoader(context);
			if (loader==null) {
				throw new ShellException("Not connected to a Cassandra "+
					"cluster. Use the \"connect\" command to connect first.");
			}

			String resourceRoot=args.get(0);

			// Canonicalize packages to resource paths
			resourceRoot=resourceRoot.replaceAll("\\.","/");

			// Build the loader
			loader.setResourceRoot(resourceRoot);
			loader.loadSchema(null);

			// Remember this in the environment
			((SchemaPlugin)getPlugin(context)).putEnvironmentValue(
				CassandraEnvironment.ENV_RESOURCE_ROOT,resourceRoot);
		
			updateStepEnvironment(context);

			context.getShell().pushPromptFormat(PROMPT);

			if (getLoader(context).getSkipExecution()) {
				context.printf("Warning: skip execution is set to %s. "+
					"Use \"skip %s\" to toggle.\n",
					getLoader(context).getSkipExecution() ? "on" : "off",
					!getLoader(context).getSkipExecution() ? "off" : "on");
			}
		}
		catch (IOException e) {
			throw new ShellException("Could not load CQL resources",e);
		}
		catch (IndexOutOfBoundsException e) {
			throw new IllegalArgumentException(
				"Expected the resource of the root schema file as the only "+
				"argument");
		}
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private static final String PROMPT=
		"[{"+CassandraEnvironment.ENV_RESOURCE_ROOT+"%s}#{step%d}] ";
}
