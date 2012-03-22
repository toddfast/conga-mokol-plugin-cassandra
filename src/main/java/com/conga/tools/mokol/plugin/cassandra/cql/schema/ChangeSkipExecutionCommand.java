package com.conga.tools.mokol.plugin.cassandra.cql.schema;

import com.conga.tools.mokol.CommandContext;
import com.conga.tools.mokol.plugin.cassandra.cql.AbstractCQLCommand;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.plugin.cassandra.cql.CQLLoader;
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
	public void execute(CommandContext context, List<String> args)
			throws ShellException {

		CQLLoader loader=getLoader(context);

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

			if (loader!=null) {
				loader.setSkipExecution(flag);
			}
		}
		else {
			throw new IllegalArgumentException("Exected a single argument, "+
				"one of: [on, true, off, false]");
		}

		if (loader!=null) {
			context.printf("Skip query execution: %b\n",
				getLoader(context).getSkipExecution());
		}
		else {
			context.printf("Skip query execution: on (not connected)\n");
		}
	}
}
