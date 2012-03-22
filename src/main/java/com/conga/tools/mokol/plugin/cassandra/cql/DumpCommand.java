package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.CommandContext;
import com.conga.tools.mokol.ShellException;
import com.conga.tools.mokol.plugin.cassandra.ParsedCassandraURL;
import com.conga.tools.mokol.spi.annotation.Example;
import com.conga.tools.mokol.spi.annotation.Help;
import com.conga.tools.mokol.spi.annotation.Switch;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.Flushable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Selector;

/**
 *
 * 
 * @author Todd Fast
 */
@Help(
	value="Dumps the column family specified as the single argument",
	examples={
		@Example(
			value="<column family>",
			description="")
	}
)
public class DumpCommand extends AbstractPelopsCommand {

	/**
	 *
	 *
	 */
	@Override
	protected void execute(CommandContext context, List<String> args)
			throws ShellException {

		super.execute(context,args);

		long startTime=System.currentTimeMillis();

		String columnFamily=null;

		String switchName=null;

		for (String arg: args) {
			if (columnFamily!=null) {
				throw new IllegalArgumentException("This command requires "+
					"a single argument for column family");
			}

			columnFamily=arg;
		}

		if (getDataFileName()==null
				|| getDataFileName().trim().isEmpty()) {
			throw new IllegalArgumentException(
				"Invalid out file name \""+getDataFileName()+"\"");
		}

		if (columnFamily==null) {
			throw new IllegalArgumentException("This command requires "+
				"a single argument for column family");
		}

		final String COLUMN_FAMILY=columnFamily;
		final int BATCH_SIZE=getBatchSize();
		final int TIMEOUT=getTimeout();

		FileWriter metadataFileWriter=null;
		FileWriter dataFileWriter=null;

		BufferedWriter metadataWriter=null;
		BufferedWriter dataWriter=null;

		int numRows=0;
		try {
			context.printf("Dumping column family \"%s\"\n",COLUMN_FAMILY);

			ParsedCassandraURL parsedURL=
				getLoader(context).getParsedURL();

			Selector selector=createSelector();

			// Get an iterator over all rows
			Iterator<Entry<Bytes,List<Column>>> rowIterator=
				selector.iterateColumnsFromRows(COLUMN_FAMILY,
					Bytes.EMPTY,
					BATCH_SIZE,ConsistencyLevel.ANY);

			File metadataFile=new File(ensurePath(getMetadataFileName()));
			boolean metadataFileExists=metadataFile.createNewFile();

			File dataFile=new File(ensurePath(getDataFileName()));
			boolean dataFileExists=dataFile.createNewFile();

			// Construct file writers
			metadataWriter=new BufferedWriter(
				metadataFileWriter=new FileWriter(metadataFile,false));
			dataWriter=new BufferedWriter(
				dataFileWriter=new FileWriter(dataFile,false));

			// Process each row
			for (Entry<Bytes,List<Column>> row; rowIterator.hasNext(); ) {
				row=rowIterator.next();

				String rowKey=row.getKey().toUTF8();

				// Write the column name into the metadata
				metadataWriter.append(
					format("rowKey",true,getTruncateNameLength()));

				// Write the coumn value into the data
				dataWriter.append(
					format(rowKey,true,getTruncateValueLength()));

//context.printf("\tDumping row \"%s\" (num columns = %d)\n",rowKey,row.getValue().size());

				// Dump all the columns for the row
				int columnCount=0;
				for (Column column: row.getValue()) {

					metadataWriter.append("\t");
					dataWriter.append("\t");

					// Write the column name into the metadata
					metadataWriter.append(
						format(Bytes.toUTF8(column.getName()),
							true,getTruncateNameLength()));

					// Write the coumn value into the data
					dataWriter.append(
						format(Bytes.toUTF8(column.getValue()),
							true,getTruncateValueLength()));

					columnCount++;
				}

				// Finish off the row line
				metadataWriter.append("\n");
				dataWriter.append("\n");

				numRows++;

				// Print some status to the console and flush
				if (numRows % BATCH_SIZE==0) {
					context.printf(".");
					dataWriter.flush();
					metadataWriter.flush();
				}

				// Stop if we've reached the row limit
				if (getRowLimit() > 0 && numRows >= getRowLimit()) {
					break;
				}
			}
		}
		catch (Exception e) {
			throw new RuntimeException(
				"Failed to dump column family \""+COLUMN_FAMILY+"\": "+
				e.getMessage(),e);
		}
		finally {
			flush(metadataWriter);
			close(metadataWriter);

			flush(dataWriter);
			close(dataWriter);

			// Finish off the status output
			context.printf("\n");
			context.printf("%d rows dumped in %fms.\n\n",numRows,
				(System.currentTimeMillis()-startTime));
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
	private void flush(Flushable flushable) {
		if (flushable!=null) {
			try {
				flushable.flush();
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




	////////////////////////////////////////////////////////////////////////////
	// Switches
	////////////////////////////////////////////////////////////////////////////

//	/**
//	 *
//	 *
//	 */
//	public int getTimeout() {
//		return timeout;
//	}
//
//
//	/**
//	 *
//	 *
//	 */
//	@Switch
//	public void setTimeout(int value) {
//		timeout=value;
//	}


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
	@Switch
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
	@Switch
	public void setBatchSize(int value) {
		batchSize=value;
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
	@Switch(name="to", abbreviation="f")
	public void setDataFileName(String value) {
		dataFileName=value;
	}


	/**
	 *
	 *
	 */
	public String getMetadataFileName() {
		if (metadataFileName==null) {
			return getDataFileName()+".metadata.tsv";
		}

		return metadataFileName;
	}


	/**
	 *
	 *
	 */
	@Switch
	public void setMetadataFileName(String value) {
		metadataFileName=value;
	}


	/**
	 *
	 *
	 */
	public int getTruncateNameLength() {
		return truncateNameLength;
	}


	/**
	 *
	 *
	 */
	@Switch(name="truncateName", abbreviation="tn")
	@Help("The length at which to truncate column names")
	public void setTruncateNameLength(@Help("length") int value) {
		truncateNameLength=value;
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

	private int timeout=30000;
	private int rowLimit=-1;
	private int batchSize=100;
	private String dataFileName="data.tsv";
	private String metadataFileName;
	private int truncateNameLength=64;
	private int truncateValueLength=128;
	private boolean replaceEscapedChars=true;
}
