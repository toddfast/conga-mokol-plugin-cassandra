package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.spi.Command;
import com.conga.tools.mokol.spi.CommandContext;
import com.conga.tools.mokol.spi.CommandFactory;
import com.conga.tools.mokol.spi.annotation.Example;
import com.conga.tools.mokol.spi.annotation.Help;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 *
 * 
 * @author Todd Fast
 */
@Help(
	value="Execute a CQL mutation statement",
	examples={
//		@Example(
//			value="...",
//			description="Make an update"
//		)
	}
)
public class CQLUpdateCommand extends AbstractCQLCommand {

	/**
	 *
	 *
	 */
	public CQLUpdateCommand(Verb verb) {
		super();

		if (verb==null) {
			throw new IllegalArgumentException(
				"Parameter \"verb\" cannot be null");
		}

		this.verb=verb;
	}


	/**
	 *
	 *
	 */
	public Verb getVerb() {
		return verb;
	}


	/**
	 *
	 *
	 */
	@Override
	public void doExecute(CommandContext context, List<String> args)
			throws ShellException {

		CQLLoader loader=getLoader(context);

		StringBuilder cqlBuilder=new StringBuilder(
			getVerb().toString().toLowerCase());

		String switchName=null;

		for (String arg: args) {
			cqlBuilder
				.append(" ")
				.append(arg);
		}

		String cql=cqlBuilder.toString();

		Connection connection=loader.getConnection();
		try {
			PreparedStatement statement=connection.prepareStatement(cql);

			// Execute the statement
			boolean result=statement.execute();

			if (result) {
				context.printf("Update successful.\n");
			}
			else {
				context.printf("Update failed.\n");
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
	public String getUsage() {
		return "Execute a CQL "+getVerb()+" statement. Switches: "+
			"none";
	}




	////////////////////////////////////////////////////////////////////////////
	// Inner types
	////////////////////////////////////////////////////////////////////////////

	/**
	 *
	 *
	 */
	public static enum Verb {
		UPDATE,
		DELETE
	}


	/**
	 *
	 *
	 */
	public static class Factory extends CommandFactory {

		public Factory(Verb action) {
			super();
			this.action=action;
		}


		@Override
		public Class<? extends Command> getCommandClass(CommandContext context) {
			return CQLUpdateCommand.class;
		}

		@Override
		public Command newInstance(CommandContext context)
				throws ShellException {
			return new CQLUpdateCommand(action);
		}

		private Verb action;
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private Verb verb;
}
