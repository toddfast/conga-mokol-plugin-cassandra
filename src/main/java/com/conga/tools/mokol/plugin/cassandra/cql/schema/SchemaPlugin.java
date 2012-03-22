package com.conga.tools.mokol.plugin.cassandra.cql.schema;

import com.conga.tools.mokol.CommandContext;
import com.conga.tools.mokol.plugin.cassandra.CassandraPluginBase;
import com.conga.tools.mokol.plugin.cassandra.cql.ConnectCommand;
import com.conga.tools.mokol.plugin.cassandra.cql.DisconnectCommand;
import com.conga.tools.mokol.spi.annotation.Command;
import com.conga.tools.mokol.spi.annotation.Plugin;
import java.util.Map;

/**
 *
 * 
 * @author Todd Fast
 */
@Plugin(
	commands={
		@Command(alias="connect", command=ConnectCommand.class),
		@Command(alias="disconnect", command=DisconnectCommand.class),
		@Command(alias="schema", command=SchemaCommand.class),
		@Command(alias="skip", command=ChangeSkipExecutionCommand.class),
		@Command(alias="steps", command=ShowStepsCommand.class),
		@Command(alias="step", command=StepCommand.class),
		@Command(alias="cs", command=StepCommand.class),
		@Command(alias="up", command=UpCommand.class),
		@Command(alias="++", command=UpCommand.class),
		@Command(alias="down", command=DownCommand.class),
		@Command(alias="--", command=DownCommand.class)
	})
public class SchemaPlugin extends CassandraPluginBase {

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
	@Command(alias="load")
	public LoadCommand load(CommandContext context) {
		return new LoadCommand(LoadCommand.Action.NOTHING);
	}


	/**
	 *
	 *
	 */
	@Command(alias="load++")
	public LoadCommand loadPostIncrement(CommandContext context) {
		return new LoadCommand(LoadCommand.Action.POST_INCREMENT);
	}


	/**
	 *
	 *
	 */
	@Command(alias="load--")
	public LoadCommand loadPostDecrement(CommandContext context) {
		return new LoadCommand(LoadCommand.Action.POST_DECREMENT);
	}


	/**
	 *
	 *
	 */
	@Command(alias="++load")
	public LoadCommand loadPreIncrement(CommandContext context) {
		return new LoadCommand(LoadCommand.Action.PRE_INCREMENT);
	}


	/**
	 *
	 *
	 */
	@Command(alias="--load")
	public LoadCommand loadPreDecrement(CommandContext context) {
		return new LoadCommand(LoadCommand.Action.PRE_DECREMENT);
	}


	/**
	 *
	 *
	 */
	@Command(alias="drop")
	public RevertCommand drop(CommandContext context) {
		return new RevertCommand(RevertCommand.Action.NOTHING);
	}


	/**
	 *
	 *
	 */
	@Command(alias="drop++")
	public RevertCommand dropPostIncrement(CommandContext context) {
		return new RevertCommand(RevertCommand.Action.POST_INCREMENT);
	}


	/**
	 *
	 *
	 */
	@Command(alias="drop--")
	public RevertCommand dropPostDecrement(CommandContext context) {
		return new RevertCommand(RevertCommand.Action.PRE_INCREMENT);
	}


	/**
	 *
	 *
	 */
	@Command(alias="++drop")
	public RevertCommand dropPreIncrement(CommandContext context) {
		return new RevertCommand(RevertCommand.Action.PRE_INCREMENT);
	}


	/**
	 *
	 *
	 */
	@Command(alias="--drop")
	public RevertCommand dropPreDecrement(CommandContext context) {
		return new RevertCommand(RevertCommand.Action.PRE_DECREMENT);
	}
}
