package com.conga.tools.mokol.plugin.cassandra.cql.schema;

import com.conga.tools.mokol.plugin.cassandra.cql.AbstractCQLCommand;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.plugin.cassandra.cql.CQLLoader;
import com.conga.tools.mokol.spi.Command;
import com.conga.tools.mokol.spi.CommandContext;
import com.conga.tools.mokol.spi.CommandFactory;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author Todd Fast
 */
public class RevertCommand extends AbstractCQLCommand {

	/**
	 *
	 *
	 * @param action
	 */
	public RevertCommand(Action action) {
		super();
		this.action=action;
	}


	/**
	 *
	 *
	 */
	@Override
	public void doExecute(CommandContext context, List<String> args)
			throws ShellException {

		CQLLoader loader=getLoader(context);

		try {

			switch (action) {
				case PRE_INCREMENT:
					context.getShell().executeCommand("up",
						Collections.<String>emptyList());
					break;

				case PRE_DECREMENT:
					context.getShell().executeCommand("down",
						Collections.<String>emptyList());
					break;
			}

			if (args.isEmpty()) {
				boolean result=loader.revert();
				if (!result) {
					context.printf("Nothing to revert\n");
				}
			}
			else
			if (args.size()==1) {
				if (args.get(0).equals("all")) {
					loader.revertAll();
				}
			}
			else {
				throw new IllegalArgumentException("Wrong number of arguments");
			}

			switch (action) {
				case POST_INCREMENT:
					context.getShell().executeCommand("up",
						Collections.<String>emptyList());
					break;

				case POST_DECREMENT:
					context.getShell().executeCommand("down",
						Collections.<String>emptyList());
					break;
			}
		}
		catch (SQLException e) {
			throw new ShellException("Could not execute step");
		}
		finally {
			updateStepEnvironment(context);
		}
	}


	/**
	 *
	 *
	 */
	public String getUsage() {
		switch (action) {
			case PRE_INCREMENT:
				return "Move up one step and then revert the current step";

			case PRE_DECREMENT:
				return "Move down one step and then revert the current step";

			case POST_INCREMENT:
				return "Revert the current step and then move up one step";

			case POST_DECREMENT:
				return "Revert the current step and then move down one step";

			case NOTHING:
			default:
				return "Revert the current step";
		}
	}




	////////////////////////////////////////////////////////////////////////////
	// Inner types
	////////////////////////////////////////////////////////////////////////////

	/**
	 *
	 *
	 */
	public static enum Action {
		NOTHING,
		PRE_INCREMENT,
		POST_INCREMENT,
		PRE_DECREMENT,
		POST_DECREMENT
	}


	/**
	 *
	 *
	 */
	public static class Factory extends CommandFactory {

		public Factory(Action action) {
			super();
			this.action=action;
		}

		@Override
		public Class<? extends Command> getCommandClass(CommandContext context) {
			return RevertCommand.class;
		}

		@Override
		public Command newInstance(CommandContext context)
				throws ShellException {
			return new RevertCommand(action);
		}

		private Action action;
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private Action action;
}
