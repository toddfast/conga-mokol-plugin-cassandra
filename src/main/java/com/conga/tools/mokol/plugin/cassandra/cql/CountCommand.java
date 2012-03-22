package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.CommandContext;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.spi.annotation.Help;
import com.conga.tools.mokol.spi.annotation.Switch;
import com.conga.tools.mokol.util.ByteArrayUtil;
import java.util.List;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Selector;

/**
 *
 * 
 * @author Todd Fast
 */
@Help("Counts the number of columns per row of the specified column family")
public class CountCommand extends AbstractPelopsCommand {

	/**
	 *
	 *
	 */
	@Override
	public void execute(CommandContext context, List<String> args)
			throws ShellException {

		super.execute(context,args);

		long startTime=System.currentTimeMillis();

		String columnFamily=null;

		for (String arg: args) {
			if (columnFamily!=null) {
				throw new IllegalArgumentException("Expected the column "+
					"family as a single argument");
			}

			columnFamily=arg;
		}

		if (columnFamily==null || columnFamily.trim().isEmpty()) {
			throw new IllegalArgumentException("Expected the column "+
				"family as a single argument");
		}

		int numRows=0;
		try {
			Selector selector=createSelector();

			// Get an iterator over all rows
			Iterator<Map.Entry<Bytes,List<Column>>> rowIterator=
				selector.iterateColumnsFromRows(columnFamily,
					Bytes.EMPTY,
					getBatchSize(),
					ConsistencyLevel.ANY);

			for (int i=0; i<getRowLimit() && rowIterator.hasNext(); i++) {
				Map.Entry<Bytes,List<Column>> entry=rowIterator.next();
				printRow(context,i,entry.getKey(),entry.getValue());
			}
		}
		catch (Exception e) {
			throw new ShellException(
				"Failed to count column family \""+columnFamily+"\": "+
				e.getMessage(),e);
		}
		finally {
			// Finish off the status output
			context.printf("\n");
			context.printf("%d rows counted in %fms.\n\n",numRows,
				(System.currentTimeMillis()-startTime));
		}



//		String columnFamily=null;
//
//		String switchName=null;
//
//		for (String arg: args) {
//			if (switchName!=null) {
//				handleSwitch(switchName,arg);
//				switchName=null;
//			}
//			else
//			if (arg.startsWith("--")) {
//				switchName=arg.substring(2);
//				// The next arg will be the switch value
//			}
//			else {
//				if (columnFamily!=null) {
//					super.wrongNumberOfParameters(1,1,2);
//				}
//
//				columnFamily=arg;
//			}
//		}
//
//		if (columnFamily==null) {
//			super.wrongNumberOfParameters(1,1,0);
//		}
//
//		CQLLoader loader=getLoader(context);
//
//		// Select all columns
//		StringBuilder cqlBuilder=
//			new StringBuilder("select * from "+columnFamily+
//			" limit "+getRowLimit());
//		String cql=cqlBuilder.toString();
//
//		Connection connection=loader.getConnection();
//		try {
//			PreparedStatement statement=connection.prepareStatement(cql);
//
//			CResultSet resultSet=(CResultSet)statement.executeQuery();
//
//			// If isResultSet==true, result is a ResultSet
//			// If isResultSet==false, result is an update count, or
//			// nothing
//
//			if (resultSet!=null) {
//				int numRows=printResultSet(context,cql,resultSet);
//				context.printf("%d rows returned.\n\n",numRows);
//			}
//			else {
//				context.printf("No result set returned.\n");
//			}
//		}
//		catch (Exception e) {
//			throw new RuntimeException(
//				"Failed to execute the CQL query \""+cql+"\": "+
//				e.getMessage(),e);
//		}
	}


	/**
	 *
	 *
	 */
	private void printRow(CommandContext context, int rowNum, Bytes rowKeyBytes,
			List<Column> columns)
			throws SQLException {

		String rowKey=null;
		try {
			rowKey=rowKeyBytes.toUTF8();
//			ByteBufferUtil.string(ByteBuffer.wrap(rowKeyBytes));
		}
		catch (Exception e) {
			rowKey=ByteArrayUtil.toHex(rowKeyBytes.toByteArray());
		}

		int maxNameWidth=Math.max(6,rowKey.length());

		final String SEPARATOR=" | ";
//		final String SEPARATOR="\t";

//		String format="%d => "+
		String format="%d"+
			SEPARATOR+
			"%"+maxNameWidth+"s"+
			SEPARATOR+
			"%-10d";

		int nameByteCount=0;
		int valueByteCount=0;
		if (getIncludeByteCounts()) {
			format+=
				SEPARATOR+
				"%-10d"+
				SEPARATOR+
				"%-10d"+
				SEPARATOR+
				"%-10d";

			for (Column column: columns) {
				byte[] bytes=column.getName();
				if (bytes!=null)
					nameByteCount+=bytes.length;

				bytes=column.getValue();
				if (bytes!=null)
					valueByteCount+=bytes.length;
			}
		}

		format+="\n";

		context.printf(format,
			rowNum+1,
			format(rowKey,true,getTruncateKeyLength()),
			columns.size(),
			nameByteCount,
			valueByteCount,
			nameByteCount+valueByteCount);
	}


	/**
	 *
	 *
	 */
	private String format(String value, boolean replaceEscaped, int maxLength) {

		if (value==null) {
			return "null";
		}

		if (replaceEscaped) {
			value=value.replaceAll("\t","\\\\t");
			value=value.replaceAll("\n","\\\\n");
			value=value.replaceAll("\r","\\\\r");
		}

		if (maxLength > 0) {
			if (value.length() > maxLength) {
				value=value.substring(0,maxLength-6)+"..."+
					value.substring(value.length()-3);
			}
		}

		return value;
	}


	/**
	 *
	 *
	 */
	public int getRowLimit() {
		return rowLimit;
	}


	/**
	 *
	 *
	 */
	@Switch(abbreviation="l", name="limit")
	@Help("The number of rows over which to count columns")
	public void setRowLimit(int value) {
		rowLimit=value;
	}


	/**
	 *
	 *
	 */
	public int getBatchSize() {
		return batchSize;
	}


	/**
	 *
	 *
	 */
	@Switch(abbreviation="b", name="batchSize")
	@Help("The number of records to retrieve for each request. This is "+
		"useful to control potential timeouts.")
	public void setBatchSize(int value) {
		batchSize=value;
	}


	/**
	 *
	 *
	 */
	public int getTruncateKeyLength() {
		return truncateKeyLength;
	}


	/**
	 *
	 *
	 */
	@Switch(name="truncateKey", abbreviation="tk")
	@Help("The length at which to truncate the row key for display")
	public void setTruncateNameLength(@Help("length") int value) {
		truncateKeyLength=value;
	}


	/**
	 *
	 *
	 */
	public boolean getReplaceEscapedChars() {
		return replaceEscapedChars;
	}


	/**
	 *
	 *
	 */
	@Switch(name="expand", abbreviation="e")
	@Help("Expand normally escaped characters like \\t and \\n")
	public void setReplaceEscapedChars() {
		replaceEscapedChars=false;
	}


	/**
	 *
	 *
	 */
	public boolean getIncludeByteCounts() {
		return includeByteCounts;
	}


	/**
	 *
	 *
	 */
	@Switch(name="countBytes", abbreviation="c")
	@Help("Include byte count for each column's name and value")
	public void setIncludeByteCounts() {
		includeByteCounts=true;
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private int rowLimit=100;
	private int batchSize=100;
	private int truncateKeyLength=64;
	private int truncateValueLength=128;
	private boolean replaceEscapedChars=true;
	private boolean includeByteCounts=false;
}
