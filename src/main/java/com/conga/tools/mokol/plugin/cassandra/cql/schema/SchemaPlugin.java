package com.conga.tools.mokol.plugin.cassandra.cql.schema;

import com.conga.tools.mokol.Shell;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.plugin.cassandra.cql.ConnectCommand;
import com.conga.tools.mokol.plugin.cassandra.cql.DisconnectCommand;
import com.conga.tools.mokol.spi.AbstractPlugin;
import com.conga.tools.mokol.spi.CommandClassFactory;
import java.util.Map;

/**
 *
 * @author Todd Fast
 */
public class SchemaPlugin extends AbstractPlugin {

	/**
	 *
	 *
	 */
	@Override
	public String getName() {
		return "Cassandra Schema Loader";
	}


	/**
	 *
	 *
	 */
	@Override
	protected Map<String,Object> getEnvironment() {
		return super.getEnvironment();
	}


	/**
	 *
	 *
	 */
	@Override
	protected <T> T getEnvironmentValue(String key, Class<T> clazz) {
		return super.getEnvironmentValue(key,clazz);
	}


	/**
	 *
	 *
	 */
	@Override
	protected void putEnvironmentValue(String key, Object value) {
		super.putEnvironmentValue(key,value);
	}


	/**
	 *
	 *
	 */
	@Override
	protected void removeEnvironmentValue(String key) {
		super.removeEnvironmentValue(key);
	}


	/**
	 *
	 *
	 */
	@Override
	public void initialize(Shell shell) throws ShellException {

		shell.aliasCommand("connect",this,
			new CommandClassFactory(ConnectCommand.class));
		shell.aliasCommand("disconnect",this,
			new CommandClassFactory(DisconnectCommand.class));

		shell.aliasCommand("schema",this,
			new CommandClassFactory(SchemaCommand.class));

		shell.aliasCommand("skip",this,
			new CommandClassFactory(ChangeSkipExecutionCommand.class));

		shell.aliasCommand("steps",this,
			new CommandClassFactory(ShowStepsCommand.class));

		// Navigation
		shell.aliasCommand("step",this,
			new CommandClassFactory(StepCommand.class));
		shell.aliasCommand("cs",this,
			new CommandClassFactory(StepCommand.class));

		shell.aliasCommand("up",this,
			new CommandClassFactory(UpCommand.class));
		shell.aliasCommand("++",this,
			new CommandClassFactory(UpCommand.class));
		shell.aliasCommand("down",this,
			new CommandClassFactory(DownCommand.class));
		shell.aliasCommand("--",this,
			new CommandClassFactory(DownCommand.class));

		shell.aliasCommand("load",this,
			new LoadCommand.Factory(LoadCommand.Action.NOTHING));
		shell.aliasCommand("load++",this,
			new LoadCommand.Factory(LoadCommand.Action.POST_INCREMENT));
		shell.aliasCommand("load--",this,
			new LoadCommand.Factory(LoadCommand.Action.POST_DECREMENT));
		shell.aliasCommand("++load",this,
			new LoadCommand.Factory(LoadCommand.Action.PRE_INCREMENT));
		shell.aliasCommand("--load",this,
			new LoadCommand.Factory(LoadCommand.Action.PRE_DECREMENT));

		shell.aliasCommand("drop",this,
			new RevertCommand.Factory(RevertCommand.Action.NOTHING));
		shell.aliasCommand("drop++",this,
			new RevertCommand.Factory(RevertCommand.Action.POST_INCREMENT));
		shell.aliasCommand("drop--",this,
			new RevertCommand.Factory(RevertCommand.Action.POST_DECREMENT));
		shell.aliasCommand("++drop",this,
			new RevertCommand.Factory(RevertCommand.Action.PRE_INCREMENT));
		shell.aliasCommand("--drop",this,
			new RevertCommand.Factory(RevertCommand.Action.PRE_DECREMENT));
	}
}
