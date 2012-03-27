package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.CommandContext;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.plugin.cassandra.ParsedCassandraURL;
import com.conga.tools.mokol.spi.annotation.Help;
import com.conga.tools.mokol.spi.annotation.Switch;
import java.util.List;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.OperandPolicy;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.RowDeletor;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool;

/**
 *
 * 
 * @author Todd Fast
 */
public abstract class AbstractPelopsCommand extends AbstractCQLCommand {

	/**
	 *
	 *
	 */
	@Override
	protected void execute(CommandContext context, List<String> args)
			throws ShellException {
		initializePelops(context);
	}


	/**
	 *
	 *
	 */
	protected void initializePelops(CommandContext context)
			throws ShellException {

		setPoolName(context.getCommandAlias());

		if (Pelops.getDbConnPool(context.getCommandAlias())==null) {

			ParsedCassandraURL parsedURL=
				getLoader(context).getParsedURL();

//System.out.format("Cassandra instance: server = %s, port %d",
//	parsedURL.getServer(),parsedURL.getPort());

			Cluster cluster=new Cluster(parsedURL.getServer(),
				parsedURL.getPort(),getTimeout(),false);

			CommonsBackedPool.Policy policy=
				new CommonsBackedPool.Policy(cluster);
	//			policy.setMaxWaitForConnection(30000);

			OperandPolicy operandPolicy=new OperandPolicy();
			operandPolicy.setMaxOpRetries(5);

			Pelops.addPool(getPoolName(),cluster,parsedURL.getKeyspace(),
				policy,operandPolicy);

//			IThriftPool pool=Pelops.getDbConnPool(getPoolName());
//			pool.getConnection().getAPI().
		}
	}


	/**
	 *
	 *
	 */
	protected Selector createSelector() {
		Selector selector=Pelops.createSelector(getPoolName());
		return selector;
	}


	/**
	 *
	 *
	 */
	protected Mutator createMutator() {
		return Pelops.createMutator(getPoolName());
	}


	/**
	 *
	 *
	 */
	protected RowDeletor createRowDeletor() {
		return Pelops.createRowDeletor(getPoolName());
	}


	/**
	 *
	 *
	 */
	protected String getPoolName() {
		return poolName;
	}


	/**
	 *
	 *
	 */
	private void setPoolName(String value) {
		this.poolName=value;
	}




	////////////////////////////////////////////////////////////////////////////
	// Switches
	////////////////////////////////////////////////////////////////////////////

	/**
	 *
	 *
	 */
	public int getTimeout() {
		return timeout;
	}


	/**
	 *
	 *
	 */
	@Switch
	@Help("Connection timeout")
	public void setTimeout(@Help("milliseconds") int value) {
		timeout=value;
	}


//	/**
//	 *
//	 *
//	 */
//	public int getRowLimit() {
//		return rowLimit;
//	}
//
//
//	/**
//	 *
//	 *
//	 */
//	@Switch
//	public void setRowLimit(int value) {
//		rowLimit=value;
//	}
//
//
//	/**
//	 *
//	 *
//	 */
//	public int getBatchSize() {
//		return batchSize;
//	}
//
//
//	/**
//	 *
//	 *
//	 */
//	@Switch
//	public void setBatchSize(int value) {
//		batchSize=value;
//	}
//
//
//	/**
//	 *
//	 *
//	 */
//	public int getTruncateNameLength() {
//		return truncateNameLength;
//	}
//
//
//	/**
//	 *
//	 *
//	 */
//	@Switch(name="truncateName", abbreviation="tn")
//	@Help("The length at which to truncate column names")
//	public void setTruncateNameLength(@Help("length") int value) {
//		truncateNameLength=value;
//	}
//
//
//	/**
//	 *
//	 *
//	 */
//	public int getTruncateValueLength() {
//		return truncateValueLength;
//	}
//
//
//	/**
//	 *
//	 *
//	 */
//	@Switch(name="truncateValue", abbreviation="tv")
//	@Help("The length at which to truncate column values")
//	public void setTruncateValueLength(@Help("length") int value) {
//		truncateValueLength=value;
//	}
//
//
//	/**
//	 *
//	 *
//	 */
//	public boolean getReplaceEscapedChars() {
//		return replaceEscapedChars;
//	}
//
//
//	/**
//	 *
//	 *
//	 */
//	@Switch(name="expand", abbreviation="e")
//	@Help("Expand normally escaped characters like \\t and \\n")
//	public void setReplaceEscapedChars() {
//		replaceEscapedChars=false;
//	}






	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	protected static final String ENV_CQL_LOADER_INSTANCE=
		CQLLoader.class.getName()+".instance";
	protected static final String ENV_STEP="step";

	private int timeout=30000;
//	private int rowLimit=-1;
//	private int batchSize=100;
//	private int truncateNameLength=64;
//	private int truncateValueLength=128;
//	private boolean replaceEscapedChars=true;

	private String poolName;
}
