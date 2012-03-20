package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.AnnotatedCommand;
import com.conga.tools.mokol.Shell.CommandContext;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.annotation.Help;
import com.conga.tools.mokol.annotation.Switch;
import com.conga.platform.util.Crypto;
import com.conga.platform.util.Triple;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.cql.jdbc.CResultSet;
import org.apache.cassandra.cql.jdbc.TypedColumn;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 *
 * 
 * @author Todd Fast
 */
@Help("Execute a CQL select statement")
public class SelectCommand extends AnnotatedCommand {

	/**
	 *
	 *
	 */
	@Override
	public void doExecute(CommandContext context, List<String> args)
			throws ShellException {

		CQLLoader loader=AbstractCQLCommand.getLoader(context);

		StringBuilder cqlBuilder=new StringBuilder("select");

		for (String arg: args) {
			cqlBuilder
				.append(" ")
				.append(arg);
		}

		String cql=cqlBuilder.toString();

		Connection connection=loader.getConnection();
		try {
			PreparedStatement statement=connection.prepareStatement(cql);

			CResultSet resultSet=(CResultSet)statement.executeQuery();

			// If isResultSet==true, result is a ResultSet
			// If isResultSet==false, result is an update count, or
			// nothing

			if (resultSet!=null) {
				int numRows=printResultSet(context,cql,resultSet);
				context.printf("%d rows returned.\n\n",numRows);
			}
			else {
				context.printf("No result set returned.\n");
			}
		}
		catch (Exception e) {
			throw new RuntimeException(
				"Failed to execute the CQL query \""+cql+"\": "+
				e.getMessage(),e);
		}
	}


	/**
	 *
	 *
	 */
	private int printResultSet(CommandContext context, String cql,
			CResultSet resultSet)
			throws SQLException {

		ResultSetMetaData metadata=resultSet.getMetaData();

		int columnCount=metadata.getColumnCount();
		final int MAX_COLUMNS=getMaxColumns() > 0
			? getMaxColumns() : Integer.MAX_VALUE;

		int rowNum=1;
		while (resultSet.next()) {
			columnCount=metadata.getColumnCount();

			separator(context);

			String rowKey=null;
			try {
				rowKey=ByteBufferUtil.string(
					ByteBuffer.wrap(resultSet.getKey()));
			}
			catch (CharacterCodingException e) {
//				rowKey=ByteBuffer.wrap(resultSet.getKey()).toString();
				rowKey=Crypto.toHex(resultSet.getKey());
			}

			List<Triple<String,String,String>> columns=
				new ArrayList<Triple<String,String,String>>();

			// Add a column for the row key
			final String KEY_NAME="rowKey";
			final String KEY_TYPE="(raw bytes as utf8)";

			columns.add(
				new Triple<String,String,String>(
					KEY_NAME,
					KEY_TYPE,
					rowKey));

			int maxNameWidth=KEY_NAME.length();
			int maxTypeWidth=KEY_TYPE.length();
			int maxValueWidth=rowKey.length();

			// Collect the columns and find the max field widths
			for (int i=1; i<=columnCount && i<=MAX_COLUMNS; i++) {
				String columnName=metadata.getColumnName(i);
				Object value=resultSet.getObject(i);

				// Try to handle count queries to give a proper value
				if (cql.toLowerCase().contains("count(*)")
						&& columnName.equals("count")) {
					// The problem here is that the JDBC driver tries to
					// blindly interpret the bytes as a UTF8 string. Instead,
					// we interpret it as a number. Even with this, the
					// resultSet.next() method above may blow up.
					TypedColumn typedColumn=resultSet.getColumn(i-1);
					value=ByteBuffer.wrap(
						typedColumn.getRawColumn().getValue()).getLong();
				}

				// Convert to a nice string
				String stringValue=null;
				if (value!=null) {
					if (value instanceof ByteBuffer) {
						// Just raw bytes; convert to hex
						TypedColumn typedColumn=resultSet.getColumn(i-1);
						stringValue=Crypto.toHex(
							typedColumn.getRawColumn().getValue());
					}
					else {
						stringValue=value.toString();
					}
				}

				Triple<String,String,String> column=
					new Triple<String,String,String>(
						metadata.getColumnName(i),
						metadata.getColumnTypeName(i),
						stringValue);

				maxNameWidth=Math.max(maxNameWidth,column.get1().length());
				maxTypeWidth=Math.max(maxTypeWidth,column.get2().length());

				columns.add(column);
			}

			// Print all the columns
			for (Triple<String,String,String> column: columns) {

				String format="%d => "+
					"%"+maxNameWidth+"s"+
					" | "+
					"%-"+maxTypeWidth+"s"+
					" | "+
					"%-10s"+
					" | "+
					"%-10s"+
					"\n";

				if (column.get3()!=null && column.get3().contains("\n")) {
					format="%d => "+
						"%"+maxNameWidth+"s"+
						" | "+
						"%-"+maxTypeWidth+"s"+
						" | "+
						"%-10s"+
						" | "+
						"(multiline value; see below)\n"+
						"***** begin multiline value *****\n"+
						"%-10s\n"+
						"***** end multiline value *****"+
						"\n";
				}

				int length=(column.get3()!=null
					? column.get3().length() : 0);

				String lengthString=""+length;
				if (getTruncateValueLength() > 0
						&& length > getTruncateValueLength()) {
					// Make the length as truncated
					lengthString+="*";
				}

				context.printf(format,
					rowNum,
					format(column.get1(),true,getTruncateNameLength()),
					column.get2(),
					lengthString,
					format(column.get3(),
						getReplaceEscapedChars(),getTruncateValueLength()));
			}

			if (columnCount > MAX_COLUMNS) {
				String format="%d => ...and %d more column(s)\n";
				context.printf(format,rowNum,(columnCount-MAX_COLUMNS-1));
			}

			rowNum++;
		}

		separator(context);

		return rowNum-1;
	}


	/**
	 *
	 *
	 */
	private void separator(CommandContext context) {
//		context.printf("__________________________________________"+
//			"______________________________________\n");
		context.printf("----------------------------------------"+
			"----------------------------------------\n");
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




	////////////////////////////////////////////////////////////////////////////
	// Switches
	////////////////////////////////////////////////////////////////////////////

	/**
	 *
	 *
	 */
	public int getTruncateNameLength() {
		return truncateNameLenght;
	}


	/**
	 *
	 *
	 */
	@Switch(name="truncateName", abbreviation="tn")
	@Help("The length at which to truncate column names")
	public void setTruncateNameLength(@Help("length") int value) {
		truncateNameLenght=value;
	}


	/**
	 *
	 *
	 */
	public int getTruncateValueLength() {
		return truncateValueLength;
	}


	/**
	 *
	 *
	 */
	@Switch(name="truncateValue", abbreviation="tv")
	@Help("The length at which to truncate column values")
	public void setTruncateValueLength(@Help("length") int value) {
		truncateValueLength=value;
	}


	/**
	 *
	 *
	 */
	public int getMaxColumns() {
		return maxColumns;
	}


	/**
	 *
	 *
	 */
	@Switch(name="maxColumns", abbreviation="mc")
	@Help("The max number of columns to show for each row")
	public void setMaxColumns(@Help("max") int value) {
		maxColumns=value;
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




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private int truncateNameLenght=64;
	private int truncateValueLength=128;
	private int maxColumns=-1;
	private boolean replaceEscapedChars=true;
}
