package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.plugin.cassandra.cql.AbstractCQLCommand;
import com.conga.tools.mokol.Shell.CommandContext;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.Command;
import com.conga.tools.mokol.CommandFactory;
import com.conga.platform.util.Preconditions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 *
 * 
 * @author Todd Fast
 */
public class CQLUpdateCommand extends AbstractCQLCommand {

	/**
	 *
	 *
	 */
	public CQLUpdateCommand(Verb verb) {
		super();
		Preconditions.argumentNotNull(verb,"verb");
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
	public void execute(CommandContext context, List<String> args)
			throws ShellException {

		CQLLoader loader=getLoader(context);

		StringBuilder cqlBuilder=new StringBuilder(
			getVerb().toString().toLowerCase());

		String switchName=null;

		for (String arg: args) {
			if (switchName!=null) {
				handleSwitch(switchName,arg);
				switchName=null;
			}
			else
			if (arg.startsWith("--")) {
				switchName=arg.substring(2);
				// The next arg will be the switch value
			}
			else {
				cqlBuilder
					.append(" ")
					.append(arg);
			}
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
	private void handleSwitch(String switchName, String switchValue) {
//		if (switchName.equals("t") || switchName.equals("truncate")) {
//			setNameTruncateLength(TypeConverter.asInt(switchValue));
//			setValueTruncateLength(TypeConverter.asInt(switchValue));
//		}
//		else
//		if (switchName.equals("tv") || switchName.equals("truncateValue")) {
//			setValueTruncateLength(TypeConverter.asInt(switchValue));
//		}
//		else
//		if (switchName.equals("tn") || switchName.equals("truncateName")) {
//			setNameTruncateLength(TypeConverter.asInt(switchValue));
//		}
//		else
//		if (switchName.equals("e") || switchName.equals("escape")) {
//			setReplaceEscapedChars(TypeConverter.asBoolean(switchValue));
//		}
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
		INSERT,
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
