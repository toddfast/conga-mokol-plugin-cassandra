package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.CommandContext;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.util.ByteArrayUtil;
import com.conga.tools.mokol.util.TypeConverter;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.cql.jdbc.CResultSet;
import org.apache.cassandra.cql.jdbc.TypedColumn;
import org.scale7.cassandra.pelops.Bytes;

/**
 *
 * 
 * @author Todd Fast
 */
public class ExportCommand extends AbstractCQLCommand {

	/**
	 *
	 *
	 */
	@Override
	public void execute(CommandContext context, List<String> args)
			throws ShellException {

		CQLLoader loader=getLoader(context);

		StringBuilder cqlBuilder=new StringBuilder("select");

		String switchName=null;

		for (String arg: args) {
			if (switchName!=null) {
				handleSwitch(switchName,arg);
				switchName=null;
			}
			else
			if (arg.startsWith("--")) {
				switchName=arg.substring(2);
				// The next arg will be the switch value
			}
			else {
				cqlBuilder
					.append(" ")
					.append(arg);
			}
		}

		String cql=cqlBuilder.toString();

		Connection connection=loader.getConnection();
		try {
			context.printf("Exporting results from query \"%s\"\n",cql);

			PreparedStatement statement=connection.prepareStatement(cql);

			CResultSet resultSet=(CResultSet)statement.executeQuery();

			// If isResultSet==true, result is a ResultSet
			// If isResultSet==false, result is an update count, or
			// nothing

			if (resultSet!=null) {
				if (getDataFileName()==null
						|| getDataFileName().trim().isEmpty()) {
					throw new IllegalArgumentException(
						"Invalid out file name \""+getDataFileName()+"\"");
				}

				int numRows=exportResultSet(context,cql,resultSet);
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
	private void handleSwitch(String switchName, String switchValue) {
		if (switchName.equals("f") || switchName.equals("to")) {
			setDataFileName(switchValue);
			setMetadataFileName(switchValue+".metadata");
		}
		else
		if (switchName.equals("t") || switchName.equals("truncate")) {
			setNameTruncateLength(TypeConverter.asInt(switchValue));
			setValueTruncateLength(TypeConverter.asInt(switchValue));
		}
		else
		if (switchName.equals("tv") || switchName.equals("truncateValue")) {
			setValueTruncateLength(TypeConverter.asInt(switchValue));
		}
		else
		if (switchName.equals("tn") || switchName.equals("truncateName")) {
			setNameTruncateLength(TypeConverter.asInt(switchValue));
		}
//		else
//		if (switchName.equals("e") || switchName.equals("escape")) {
//			setReplaceEscapedChars(TypeConverter.asBoolean(switchValue));
//		}
	}


	/**
	 *
	 *
	 */
	private int exportResultSet(CommandContext context, String cql,
			CResultSet resultSet)
			throws IOException, SQLException {

		FileWriter dataFileWriter=null;
		FileWriter metadataFileWriter=null;
		try {
			File dataFile=new File(ensurePath(getDataFileName()));
			boolean dataFileExists=dataFile.createNewFile();
			File metadataFile=new File(ensurePath(getMetadataFileName()));
			boolean metadataFileExists=metadataFile.createNewFile();

			// Construct file writers
			BufferedWriter dataWriter=new BufferedWriter(
				dataFileWriter=new FileWriter(dataFile,false));
			BufferedWriter metadataWriter=new BufferedWriter(
				metadataFileWriter=new FileWriter(metadataFile,false));

			ResultSetMetaData metadata=resultSet.getMetaData();

			int columnCount=metadata.getColumnCount();

			int rowNum=1;
			while (resultSet.next()) {
				columnCount=metadata.getColumnCount();

				String rowKey=null;
				try {
	//				final Charset charset=Charset.forName("UTF-8");
	//				rowKey=charset.newDecoder().decode(
	//					ByteBuffer.wrap(resultSet.getKey())).toString();
					rowKey=Bytes.toUTF8(resultSet.getKey());
				}
				catch (Exception e) {
					rowKey=ByteArrayUtil.toHex(resultSet.getKey());
				}

				List<ColumnMetadata> columns=
					new ArrayList<ColumnMetadata>();

				// Add a column for the row key
				final String KEY_NAME="rowKey";
				final String KEY_TYPE="(raw bytes as utf8)";
				columns.add(
					new ColumnMetadata(
						KEY_NAME,
						KEY_TYPE,
						rowKey));

				// Collect the columns
				for (int i=1; i<=columnCount; i++) {
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

					ColumnMetadata column=
						new ColumnMetadata(
							metadata.getColumnName(i),
							metadata.getColumnTypeName(i),
							value.toString());

					columns.add(column);
				}

				int count=0;
				for (ColumnMetadata column: columns) {

					if (count>0) {
						metadataWriter.append("\t");
						dataWriter.append("\t");
					}

					// Write the column name into the metadata
					metadataWriter.append(
						format(column.get1(),true,getNameTruncateLength()));

					// Write the coumn value into the data
					dataWriter.append(
						format(column.get3(),true,getValueTruncateLength()));

					count++;
				}

				rowNum++;

				// Print some status to the console
				if (rowNum % 10==0) {
					context.printf(".");
				}

				// Finish off the row line
				metadataWriter.append("\n");
				dataWriter.append("\n");
			}

			metadataWriter.flush();
			dataWriter.flush();

			return rowNum-1;
		}
		finally {
			close(metadataFileWriter);
			close(dataFileWriter);

			// Finish off the status output
			context.printf("\n");
		}
	}


	/**
	 *
	 *
	 */
	private static String ensurePath(String pathName)
			throws IOException {

		return pathName;

//		File file=new File(pathName);
//
//		boolean result=false;
//		if (file.exists()) {
//			result=true;
//		}
//		else {
//			result=file.mkdirs();
//		}
//
//		if (!result) {
//			throw new IOException("Could not ensure path \""+
//				pathName+"\"");
//		}
//
//		return pathName;
	}


	/**
	 *
	 *
	 */
	private void close(Closeable closeable) {
		if (closeable!=null) {
			try {
				closeable.close();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}


	/**
	 *
	 *
	 */
	private String format(String value, boolean replaceEscaped, int maxLength) {

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
	public String getDataFileName() {
		return dataFileName;
	}


	/**
	 *
	 *
	 */
	public void setDataFileName(String value) {
		dataFileName=value;
	}


	/**
	 *
	 *
	 */
	public String getMetadataFileName() {
		return metadataFileName;
	}


	/**
	 *
	 *
	 */
	public void setMetadataFileName(String value) {
		metadataFileName=value;
	}


	/**
	 *
	 *
	 */
	public int getNameTruncateLength() {
		return nameTruncateLength;
	}


	/**
	 *
	 *
	 */
	public void setNameTruncateLength(int value) {
		nameTruncateLength=value;
	}


	/**
	 *
	 *
	 */
	public int getValueTruncateLength() {
		return valueTruncateLength;
	}


	/**
	 *
	 *
	 */
	public void setValueTruncateLength(int value) {
		valueTruncateLength=value;
	}


	/**
	 *
	 *
	 */
	public boolean getReplaceEscapedChars() {
		return replaceEscapedChars;
	}


//	/**
//	 *
//	 *
//	 */
//	public void setReplaceEscapedChars(boolean value) {
//		replaceEscapedChars=value;
//	}


	/**
	 *
	 *
	 */
	public String getUsage() {
		return "Export the results of a CQL select statement to a file. "+
			"Switches: "+
			"--f, --to <string>: output file name"+
			"--tn <int>: Truncate column names at the specified length; "+
			"--tv <int>: Truncate column values at the specified length; "+
			"";
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private String dataFileName;
	private String metadataFileName;
	private int nameTruncateLength=64;
	private int valueTruncateLength=128;
	private boolean replaceEscapedChars=true;
}
