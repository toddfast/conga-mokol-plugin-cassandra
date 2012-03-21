package com.conga.tools.mokol.plugin.cassandra.cql.schema;

import com.conga.tools.mokol.plugin.cassandra.cql.AbstractCQLCommand;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.plugin.cassandra.cql.CQLLoader;
import com.conga.tools.mokol.spi.CommandContext;
import com.conga.tools.mokol.spi.annotation.Help;
import java.util.List;

/**
 *
 * @author Todd Fast
 */
@Help("Move the current step down by one")
public class DownCommand extends AbstractCQLCommand {

	/**
	 *
	 *
	 */
	@Override
	public void doExecute(CommandContext context, List<String> args)
			throws ShellException {

		CQLLoader loader=getLoader(context);

//		try {
			if (args.size()==0) {
				loader.down();
			}
			else {
				throw new IllegalArgumentException("No arguments allowed");
			}

			updateStepEnvironment(context);
//			context.printf("Current step number: %d\n",loader.getCurrentStep());
//		}
//		catch (SQLException e) {
//			throw new ShellException("Could not execute step");
//		}
	}
}
