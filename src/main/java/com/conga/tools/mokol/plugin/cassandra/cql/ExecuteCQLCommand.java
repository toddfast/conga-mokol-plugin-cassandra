package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.spi.CommandContext;
import com.conga.tools.mokol.spi.annotation.Help;
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
	public void doExecute(CommandContext context, List<String> args)
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
}
