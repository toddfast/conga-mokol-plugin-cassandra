package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.CommandIntrospector;
import com.conga.tools.mokol.Shell.CommandContext;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.Usage;
import com.conga.tools.mokol.annotation.Help;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

/**
 *
 * 
 * @author Todd Fast
 */
@Help("Execute a raw CQL statement")
public class ExecuteCQLCommand extends AbstractCQLCommand {

	/**
	 *
	 *
	 */
	public ExecuteCQLCommand() {
		super();
	}


	/**
	 *
	 *
	 */
	@Override
	public void execute(CommandContext context, List<String> args)
			throws ShellException {

		CQLLoader loader=getLoader(context);

		StringBuilder cqlBuilder=new StringBuilder();
		for (String arg: args) {
			cqlBuilder
				.append(" ")
				.append(arg);
		}

		String cql=cqlBuilder.toString();

		Connection connection=loader.getConnection();
		try {
			Statement statement=connection.createStatement();
			boolean result=statement.execute(cql);

			// Execute the statement
			if (result) {
				context.printf("Exeution successful.\n");
			}
			else {
				context.printf("Execution failed.\n");
			}
		}
		catch (Exception e) {
			throw new RuntimeException(
				"Failed to execute the CQL query \""+cql+"\": "+
				e.getMessage(),e);
		}
	}


	/**
	 *
	 *
	 */
	@Override
	public Usage getUsage(CommandContext context) {
		if (usageDescriptor==null) {
			usageDescriptor=
				CommandIntrospector.getUsageDescriptor(context,this.getClass());
		}

		return usageDescriptor;
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private	Usage usageDescriptor;
}
