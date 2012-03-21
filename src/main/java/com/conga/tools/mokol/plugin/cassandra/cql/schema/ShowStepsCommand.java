package com.conga.tools.mokol.plugin.cassandra.cql.schema;

import com.conga.tools.mokol.plugin.cassandra.cql.AbstractCQLCommand;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.plugin.cassandra.cql.CQLLoader;
import com.conga.tools.mokol.plugin.cassandra.cql.CQLLoader.Step;
import com.conga.tools.mokol.spi.CommandContext;
import com.conga.tools.mokol.spi.annotation.Example;
import com.conga.tools.mokol.spi.annotation.Help;
import com.conga.tools.mokol.util.StringUtil;
import java.util.List;

/**
 *
 * 
 * @author Todd Fast
 */
@Help(
	value="Show the currently loaded steps",
	examples={
		@Example(
			value="load",
			description="Show all load scripts"
		),
		@Example(
			value="revert",
			description="Show all revert scripts"
		)
	}
)
public class ShowStepsCommand extends AbstractCQLCommand {

	/**
	 *
	 *
	 */
	@Override
	public void doExecute(CommandContext context, List<String> args)
			throws ShellException {

		if (args.isEmpty()) {
			showLoadSteps(context);
		}
		else
		if (args.size()==1) {
			if (args.get(0).equals("revert"))
				showRevertSteps(context);
			else
				showLoadSteps(context);
		}
		else {
			throw new IllegalArgumentException("Wrong number of arguments");
		}
	}


	/**
	 *
	 *
	 */
	private void showLoadSteps(CommandContext context)
			throws ShellException {
		CQLLoader loader=getLoader(context);
		int current=loader.getCurrentStep();

		for (int i=0; i<loader.getNumSteps(); i++) {
			showStep(context,i,i==current,getLoader(context).getStep(i));
		}

		context.printf("\n");
	}


	/**
	 *
	 *
	 */
	private void showRevertSteps(CommandContext context)
			throws ShellException {
		CQLLoader loader=getLoader(context);
		int current=loader.getCurrentStep();

		for (int i=loader.getNumSteps()-1; i>=0; i--) {
			showStep(context,i,i==current,getLoader(context).getRevertStep(i));
		}

		context.printf("\n");
	}


	/**
	 *
	 *
	 */
	private void showStep(CommandContext context, int stepNum,
			boolean current, Step step) {

		String format="\n#%d: %s\n";
		if (current) {
			format="\n#%d: %s <-- CURRENT\n";
		}

		String line=String.format(format,stepNum,step.getName());
		context.printf(line);
		context.printf(StringUtil.repeat("-",line.trim().length())+"\n");

		List<String> statements=step.getStatements();
		if (statements!=null) {
			for (String statement: statements) {
				context.printf("%5s\n",statement);
			}
		}
	}
}
