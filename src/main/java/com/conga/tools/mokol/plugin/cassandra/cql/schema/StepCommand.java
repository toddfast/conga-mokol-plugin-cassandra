package com.conga.tools.mokol.plugin.cassandra.cql.schema;

import com.conga.tools.mokol.plugin.cassandra.cql.AbstractCQLCommand;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.plugin.cassandra.cql.CQLLoader;
import com.conga.tools.mokol.plugin.cassandra.cql.CQLLoader.Step;
import com.conga.tools.mokol.spi.CommandContext;
import com.conga.tools.mokol.spi.annotation.Example;
import com.conga.tools.mokol.spi.annotation.Help;
import com.conga.tools.mokol.util.TypeConverter;
import java.util.List;

/**
 *
 * 
 * @author Todd Fast
 */
@Help(
	value="Sets the current step number / shows the current step CQL",
	examples={
		@Example(
			value="load",
			description="Show the load CQL for the current step"),
		@Example(
			value="revert]",
			description="Show the revert CQL for the current step"),
		@Example(
			value="<number>",
			description="Move to the specified step number")
	}
)
public class StepCommand extends AbstractCQLCommand {

	/**
	 *
	 *
	 */
	@Override
	public void doExecute(CommandContext context, List<String> args)
			throws ShellException {

		CQLLoader loader=getLoader(context);
		int stepNum=loader.getCurrentStep();

		if (args.isEmpty()) {
			showStep(context,stepNum,getLoader(context).getStep(stepNum));
		}
		else
		if (args.size()==1) {
			if (args.get(0).equals("load")) {
				showStep(context,stepNum,
					getLoader(context).getStep(stepNum));
			}
			else
			if (args.get(0).equals("revert")) {
				showStep(context,stepNum,
					getLoader(context).getRevertStep(stepNum));
			}
			else {
				int stepNumber=TypeConverter.asInt(args.get(0));
				loader.setCurrentStep(stepNumber);
				updateStepEnvironment(context);
			}

		}
		else {
			throw new IllegalArgumentException(
				"Expected a single argument, one of [load, revert]");
		}
	}


	/**
	 *
	 *
	 */
	private void showStep(CommandContext context, int stepNum, Step step) {
		String format="\n#%d: %s <-- CURRENT\n";

		String line=String.format(format,stepNum,step.getName());
		context.printf(line);

		StringBuilder separator=new StringBuilder();
		for (int i=0; i<line.trim().length(); i++)
			separator.append("-");

		context.printf(separator.toString()+"\n");

		List<String> statements=step.getStatements();
		for (String statement: statements) {
			context.printf("%5s\n",statement);
		}

		context.printf("\n");
	}
}
