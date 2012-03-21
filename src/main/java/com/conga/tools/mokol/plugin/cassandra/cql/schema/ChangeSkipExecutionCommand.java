package com.conga.tools.mokol.plugin.cassandra.cql.schema;

import com.conga.tools.mokol.plugin.cassandra.cql.AbstractCQLCommand;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.spi.CommandContext;
import com.conga.tools.mokol.spi.annotation.Help;
import com.conga.tools.mokol.util.TypeConverter;
import java.util.List;

/**
 *
 * @author Todd Fast
 */
@Help("Modify the skip execution flag. When set to false, queries "+
	"will not be sent to the server.")
public class ChangeSkipExecutionCommand extends AbstractCQLCommand {

	/**
	 *
	 *
	 */
	@Override
	public void doExecute(CommandContext context, List<String> args)
			throws ShellException {

		if (args.isEmpty()) {
			// Nothing
		}
		else
		if (args.size()==1) {
			boolean flag=false;
			if (args.get(0).equals("on")) {
				flag=true;
			}
			else
			if (args.get(0).equals("off")) {
				flag=false;
			}
			else {
				flag=TypeConverter.asBoolean(args.get(0));
			}

			getLoader(context).setSkipExecution(flag);
		}
		else {
			throw new IllegalArgumentException("Exected a single argument, "+
				"one of: [on, true, off, false]");
		}

		context.printf("Skip query execution: %b\n",
			getLoader(context).getSkipExecution());
	}
}
