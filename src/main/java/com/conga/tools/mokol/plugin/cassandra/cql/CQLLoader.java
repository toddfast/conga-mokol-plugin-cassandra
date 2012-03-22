package com.conga.tools.mokol.plugin.cassandra.cql;

import com.conga.tools.mokol.plugin.cassandra.ParsedCassandraURL;
import com.conga.tools.mokol.util.StringUtil;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Loads a set of numbered CQL files into a specified Cassandra node
 *
 * @author Todd Fast
 */
public class CQLLoader {

	/**
	 * 
	 * 
	 */
	public CQLLoader() {
		super();
	}


	/**
	 *
	 *
	 */
	@Override
	public String toString() {
		ParsedCassandraURL url=getParsedURL();
		StringBuilder result=new StringBuilder();
		result
			.append(url.getUser())
			.append("@")
			.append(url.getServer())
			.append(":")
			.append(url.getPort())
			.append("/")
			.append(url.getKeyspace());

		return result.toString();
	}


	/**
	 * 
	 * 
	 */
	public synchronized void openConnection()
		throws SQLException 
	{
		closeConnection();
		connection=DriverManager.getConnection(getURL());
	}


	/**
	 * 
	 * 
	 */
	public synchronized void closeConnection()
		throws SQLException
	{
		if (connection!=null) {
			connection.close();
			connection=null;
		}
	}


	/**
	 *
	 *
	 */
	public synchronized Connection getConnection() {
		return connection;
	}

	
	/**
	 * The JDBC URL for the Cassandra node
	 * 
	 */
	public synchronized String getURL() {
		return url;
	}

	
	/**
	 * 
	 * 
	 */
	public synchronized void setURL(String value) {
		url=value;
	}


	/**
	 *
	 *
	 */
	public synchronized ParsedCassandraURL getParsedURL() {
		if (parsedURL==null) {
			parsedURL=new ParsedCassandraURL(getURL());
		}

		return parsedURL;
	}

	
	/**
	 * 
	 * 
	 */
	public synchronized int getStepRange() {
		return stepRange;
	}

	
	/**
	 * 
	 * 
	 */
	public synchronized void setStepRange(int value) {
		stepRange=value;
	}


	/**
	 *
	 *
	 */
	public synchronized int getCurrentStep() {
		return currentStep;
	}


	/**
	 *
	 *
	 */
	public synchronized void setCurrentStep(int value) {
		if (value > getStepRange()-1) {
			throw new IllegalArgumentException(
				"Step number "+value+" must be less than max range "+
				getStepRange());
		}

		if (value >= getNumSteps()) {
			throw new IllegalArgumentException(
				"Value must be less than the number of steps ("+
				getNumSteps()+")");
		}

		if (value < 0) {
			throw new IllegalArgumentException(
				"Value must be equal to or greater than 0");
		}

		currentStep=value;
	}

	
	/**
	 * 
	 * 
	 */
	public synchronized Class getLoadRelativeToClass() {
		return relativeToClass;
	}


	/**
	 * 
	 * 
	 */
	public synchronized void setLoadRelativeToClass(Class clazz) {

		if (clazz==null) {
			throw new IllegalArgumentException("Parameter \"clazz\" "+
				"cannot be null");
		}

		relativeToClass=clazz;
		defaultClassLoader=clazz.getClassLoader();

		// Set the resource root too
		if (getResourceRoot()==null) {
			String name=clazz.getName();
			String path=name.substring(0,name.lastIndexOf("."));
			path.replaceAll(".","/");

			setResourceRoot(path);
		}
	}

	
	/**
	 * 
	 * 
	 */
	public synchronized String getResourceRoot() {
		return resourceRoot;
	}

	
	/**
	 * 
	 * 
	 */
	public synchronized void setResourceRoot(String value) {
		resourceRoot=value;
	}


	/**
	 *
	 *
	 */
	public synchronized boolean getSkipExecution() {
		return skipExecution;
	}


	/**
	 *
	 *
	 */
	public synchronized void setSkipExecution(boolean value) {
		skipExecution=value;
	}



	/**
	 * Load the schema steps from the current resource root
	 *
	 */
	public synchronized void loadSchema(ClassLoader classLoader)
			throws IOException {

		List<Step> steps=loadSteps(classLoader,Direction.UP);
		List<Step> revertSteps=loadSteps(classLoader,Direction.DOWN);

		if (steps==null || steps.size()==0) {
			throw new IOException("No steps were found at the resource "+
				"location \""+getResourceRoot()+"\"");
		}

		if (steps==null || revertSteps.size()==0) {
			throw new IOException("No revert steps were found at the resource "+
				"location \""+getResourceRoot()+"\"");
		}

		if (steps.size()!=revertSteps.size()) {
			throw new IOException("The number of steps and revert steps must "+
				"be equal");
		}

		numSteps=steps.size();

		// Match the revert steps
		for (int i=0; i<numSteps; i++) {
			Step step=steps.get(i);
			Step revertStep=revertSteps.get(i);
			if (!step.getName().equals(revertStep.getName()))
				throw new IllegalArgumentException(
					String.format("The load and revert names for step %d "+
						"do not match: \"%s\" != \"%s\"",
						i,step.getName(),revertStep.getName()));
		}

		stepsRef.set(steps);
		revertStepsRef.set(revertSteps);
	}


	/**
	 *
	 *
	 */
	protected List<Step> loadSteps(ClassLoader classLoader,
			Direction direction)
			throws IOException {

		List<Step> allSteps=new ArrayList<Step>();

		if (classLoader==null) {
			if (defaultClassLoader!=null) {
				classLoader=defaultClassLoader;
			}
			else {
				classLoader=CQLLoader.class.getClassLoader();
			}
		}

		for (int i=0; i<getStepRange(); i++) {

			String resourceName=getResourceName(i,direction);

			try {
				List<Step> steps=
					loadCQLStepFile(classLoader,resourceName);

				log("Loaded "+resourceName+"...");

				allSteps.addAll(steps);
			}
			catch (FileNotFoundException e) {
				// We're done
				log("File \""+resourceName+"\" not found; all steps loaded");
				break;
			}
		}

		return allSteps;
	}

	
	/**
	 * 
	 * 
	 */
	private String getResourceName(int fileNumber, Direction direction) {

		StringBuilder resourceName=new StringBuilder();
		if (getResourceRoot()!=null)
			resourceName.append(getResourceRoot());

		if (resourceName==null || resourceName.toString().trim().isEmpty())
			resourceName=new StringBuilder("");

		if (resourceName.length() > 0 && !resourceName.toString().endsWith("/"))
			resourceName.append("/");

		resourceName
			.append(pad(fileNumber,getStepRange()))
			.append(direction==Direction.DOWN ? ".revert" : "")
			.append(".cql");
		
		return resourceName.toString();
	}


	/**
	 * 
	 * 
	 */
	private String pad(int i, final int range) {
		StringBuilder result=new StringBuilder(""+i);
		while (result.length() < (""+(range-1)).length())
			result.insert(0,"0");
		
		return result.toString();
	}


	/**
	 *
	 *
	 */
	public synchronized int getNumSteps() {
		return numSteps;
	}


	/**
	 *
	 *
	 */
	public synchronized boolean hasStep(Direction direction, int stepNumber) {

//		ensureStep(direction);

		boolean result=false;

		List<Step> statements=stepsRef.get();
		if (statements!=null) {
			int totalSteps=getNumSteps();

			// Are there statements for the next step?
			switch (direction) {
				case UP: {
					result=(stepNumber >= 0)
						&& ((stepNumber) < totalSteps);
					break;
				}

				case DOWN: {
					result=(stepNumber >= 0)
						&& (stepNumber < totalSteps);
					break;
				}
			}
		}

		return result;
	}


	/**
	 *
	 *
	 */
	public synchronized boolean hasNextStep(Direction direction) {
		boolean result=false;

		List<Step> steps=stepsRef.get();
		if (steps!=null) {
			int stepNumber=getCurrentStep();
			int numSteps=getNumSteps();

			// Are there statements for the next step?
			switch (direction) {
				case UP: {
					result=(stepNumber+1 >= 0)
						&& ((stepNumber+1) < numSteps);
					break;
				}

				case DOWN: {
					result=(stepNumber > 0) 
						&& ((stepNumber-1) >= 0);
					break;
				}
			}
		}

		return result;
	}


	/**
	 *
	 *
	 */
	public synchronized Step getStep(int stepNumber) {
		if (stepNumber==UNSET_STEP)
			throw new IllegalStateException("The step number has not been set");

		List<Step> steps=stepsRef.get();
		if (steps==null
				|| stepNumber < 0
				|| stepNumber >= steps.size()) {
			return null;
		}
		else {
			return steps.get(stepNumber);
		}
	}


	/**
	 *
	 *
	 */
	public synchronized List<String> getStepStatements(int stepNumber) {
		Step step=getStep(stepNumber);
		if (step==null)
			return null;
		else
			return step.getStatements();
	}


	/**
	 *
	 *
	 */
	public synchronized Step getRevertStep(int stepNumber) {
		if (stepNumber==UNSET_STEP)
			throw new IllegalStateException("The step number has not been set");

		List<Step> steps=revertStepsRef.get();
		if (steps==null
				|| stepNumber < 0
				|| stepNumber >= steps.size()) {
			return null;
		}
		else {
			return steps.get(stepNumber);
		}
	}


	/**
	 *
	 *
	 */
	public synchronized List<String> getRevertStepStatements(int stepNumber) {
		Step step=getRevertStep(stepNumber);
		if (step==null)
			return null;
		else
			return step.getStatements();
	}


	/**
	 *
	 *
	 */
	public synchronized boolean up() {
		int stepNumber=getCurrentStep();
		if (stepNumber==UNSET_STEP) {
			if (hasStep(Direction.UP,0)) {
				setCurrentStep(0);
				return true;
			}
			else {
				return false;
			}
		}

		if (hasStep(Direction.UP,stepNumber+1)) {
			setCurrentStep(stepNumber+1);
			return true;
		}
		else {
			return false;
		}
	}


	/**
	 *
	 *
	 */
	public synchronized boolean down() {
		int stepNumber=getCurrentStep();
		if (stepNumber==UNSET_STEP) {
			if (hasStep(Direction.UP,0)) {
				setCurrentStep(0);
				return true;
			}
			else {
				return false;
			}
		}

		if (hasStep(Direction.DOWN,stepNumber-1)) {
			setCurrentStep(stepNumber-1);
			return true;
		}
		else {
			return false;
		}
	}


	/**
	 *
	 *
	 */
	public synchronized boolean load()
			throws SQLException {

		int stepNumber=getCurrentStep();
		if (hasStep(Direction.UP,stepNumber)) {
			executeStep(Direction.UP,stepNumber);
			return true;
		}
		else {
			return false;
		}
	}


	/**
	 * Starting at the current step, continues calling up() until no more steps
	 * remain
	 *
	 */
	public synchronized void loadAll()
			throws SQLException {

		setCurrentStep(0);
		while (hasStep(Direction.UP,getCurrentStep())) {
			if (!load())
				break;

			if (!up())
				break;
		}
	}


	/**
	 * Starting at the current step, continues calling down() until no more
	 * steps remain
	 *
	 */
	public synchronized boolean revert()
			throws SQLException {

		int stepNumber=getCurrentStep();
		if (hasStep(Direction.DOWN,stepNumber)) {
			executeStep(Direction.DOWN,stepNumber);
			return true;
		}
		else {
			return false;
		}
	}


	/**
	 * Starting at the current step, continues calling down() until no more
	 * steps remain
	 *
	 */
	public synchronized void revertAll()
			throws SQLException {

		setCurrentStep(getNumSteps()-1);
		while (hasStep(Direction.DOWN,getCurrentStep())) {
			if (!revert())
				break;

			if (!down())
				break;
		}
	}


	/**
	 *
	 *
	 */
	public synchronized void executeStep(Direction direction, int stepNumber)
			throws SQLException {

//		if (!hasStep(direction))
//			return;

//		ensureStep(direction);
//		int stepNumber=getCurrentStep();

		assert stepNumber!=UNSET_STEP:
			"Step number was not set";

		List<String> statements;
		switch (direction) {
			case UP:
				statements=getStepStatements(stepNumber);
				break;

			case DOWN:
				statements=getRevertStepStatements(stepNumber);
				break;

			default:
				statements=null;
		}

		if (statements!=null) {
			// Execute the statements
			log("Executing step "+stepNumber+"...");

			execute(statements);
		}
		else {
			log("Skipping empty step "+stepNumber+"...");
		}

		log("Done executing step "+stepNumber+".");
	}


	/**
	 * Execute the list of CQL statements. The list of statements is first
	 * verified before any are executed.
	 *
	 */
	public synchronized void execute(List<String> cqlStatements) {

		try {
			if (getConnection()==null || getConnection().isClosed()) {
				throw new IllegalStateException(
					"Connection has not been opened");
			}
		}
		catch (SQLException e) {
			throw new IllegalStateException("Connection has not been opened");
		}

		// Check the CQL statements for basic validity
		if (cqlStatements==null || cqlStatements.isEmpty()) {
			throw new IllegalArgumentException(
				"Parameter \"cqlStatements\" cannot be null or empty");
		}

		for (String cql: cqlStatements) {
			if (cql==null || cql.trim().isEmpty()) {
				throw new IllegalArgumentException(
					"None of the provided CQL statements can be null or empty");
			}
		}

		PreparedStatement statement;
		for (String cql: cqlStatements) {
			try {
				if (!skipExecution) {
//					log("\tExecuting CQL statement: "+cql);
					log("\t"+cql);
				}
				else {
//					log("\tSkipping CQL statement: "+cql);
					log("\t(skipped) "+cql);
				}

				statement=getConnection().prepareStatement(cql);

				boolean firstResultForm=false;

				// We allow skipping of actual execution
				if (!skipExecution)
					firstResultForm=statement.execute();

				// If firstResultForm==true, result is a ResultSet
				// If firstResultForm==false, result is an update count, or
				// nothing
			}
			catch (Exception e) {
//				e.printStackTrace();
				throw new RuntimeException(
					"Failed to execute the CQL query \""+cql+"\": "+
					e.getMessage(),e);
			}
		}
	}




	////////////////////////////////////////////////////////////////////////////
	// Static methods
	////////////////////////////////////////////////////////////////////////////

	/**
	 *
	 *
	 */
	public static List<Step> loadCQLStepFile(
			ClassLoader classLoader, String resourceName)
			throws IOException {

		if (classLoader==null)
			classLoader=StringUtil.class.getClassLoader();

		InputStream is=classLoader.getResourceAsStream(resourceName);
		try {
			if (is==null) {
				throw new FileNotFoundException(
					"Resource \""+resourceName+"\" not found");
			}

			BufferedReader reader=new BufferedReader(new InputStreamReader(is));

			Step step=new Step("(__prologue)");
			Set<String> stepNames=new HashSet<String>();
			stepNames.add(step.getName());

			List<Step> result=new ArrayList<Step>();
			StringBuilder buffer=new StringBuilder();

			// Read each line. Wrap each input line to several output lines
			// and append them to the template result.
			String line;
			while ((line=reader.readLine())!=null) {

				if (line.trim().isEmpty()) {
//					buffer.append(" ");
				}
				else
				if (line.trim().startsWith("--")) {
					// Analyze comments
					if (line.matches("^--\\s*@.*")) {

						// Special annotation comment
						int index=line.indexOf("@");
						String[] tokens=line.substring(index+1).split("\\s");
						if (tokens.length==0)
							continue;

						if (tokens[0].equals("step")) {

							if (tokens.length!=2) {
								throw new IllegalArgumentException(
									"Invalid @step directive format (\""+line+
									"\"). Must conform to \"-- @step <name>\"");
							}

							// Finish the current step
							List<String> statements=normalizeCQLBuffer(buffer);
							if (!statements.isEmpty()) {
								step.getStatements().addAll(
									normalizeCQLBuffer(buffer));
								result.add(step);
//System.out.printf("Added step %d [%s] %d\n",result.size()-1,step.getName(),statements.size());
							}
							else {
//System.out.printf("Skipped empty step [%s]\n",step.getName());
							}

							buffer=new StringBuilder();

							// New step
							String name=tokens.length>=2 ? tokens[1] : null;
							step=new Step(name);

							if (stepNames.contains(step.getName())) {
								throw new IllegalArgumentException(
									"Duplicate @step name \""+
									step.getName()+"\"");
							}

							stepNames.add(step.getName());
						}
					}
				}
				else {
					buffer.append(line).append("\n");
				}
			}

			// Finish the current step
			List<String> statements=normalizeCQLBuffer(buffer);
			if (!statements.isEmpty()) {
				step.getStatements().addAll(normalizeCQLBuffer(buffer));
				result.add(step);
//System.out.printf("Added step %d [%s] %d\n",result.size()-1,step.getName(),statements.size());
			}
			else {
//System.out.printf("Skipped empty step [%s]\n",step.getName());
			}

			return result;
		}
		finally {
			if (is!=null)
				is.close();
		}
	}


	/**
	 *
	 * 
	 */
	private static List<String> normalizeCQLBuffer(StringBuilder buffer) {
		String result=buffer.toString().replaceAll("\n","");
		result=result.replaceAll("\t"," ");
		result=result.replaceAll(";",";\n");
		if (result.trim().isEmpty())
			return Collections.emptyList();
		else
			return Arrays.asList(result.split("\n"));
	}


	/**
	 *
	 *
	 */
	public static List<String> loadCQLFile(
			ClassLoader classLoader, String resourceName)
			throws IOException {

		List<String> result=new ArrayList<String>();
		StringBuilder buffer=new StringBuilder();

		if (classLoader==null)
			classLoader=StringUtil.class.getClassLoader();

		InputStream is=classLoader.getResourceAsStream(resourceName);
		try {
			if (is==null) {
				throw new FileNotFoundException(
					"Resource \""+resourceName+"\" not found");
			}

			BufferedReader reader=new BufferedReader(new InputStreamReader(is));

			// Read each line. Wrap each input line to several output lines
			// and append them to the template result.
			String line;
			while ((line=reader.readLine())!=null) {

				// Skip comments
				if (line.trim().startsWith("--"))
					continue;

				buffer.append(line).append("\n");
			}

			String lines=buffer.toString().replaceAll("\n","");
			lines=lines.replaceAll("\t"," ");
			lines=lines.replaceAll(";",";\n");

			result.addAll(Arrays.asList(lines.split("\n")));
			return result;
		}
		finally {
			if (is!=null)
				is.close();
		}
	}


	/**
	 * 
	 * 
	 */
	private static void log(Object value, Object... params) {
		System.out.printf((value!=null ? value.toString() : "null"),params);
		System.out.println();
	}




	////////////////////////////////////////////////////////////////////////////
	// Factory methods 
	////////////////////////////////////////////////////////////////////////////

	/**
	 * 
	 * 
	 */
	public static Builder url(String url) {
		Builder builder=new Builder();
		return builder.url(url);
	}

	


	////////////////////////////////////////////////////////////////////////////
	// Inner types
	////////////////////////////////////////////////////////////////////////////

	/**
	 *
	 *
	 */
	public static enum Direction {
		UP,
		DOWN
	}


	/**
	 *
	 *
	 */
	public static class Step {

		/**
		 *
		 *
		 */
		protected Step(String name) {
			this(name,new ArrayList<String>());
		}

		/**
		 *
		 *
		 */
		protected Step(String name, List<String> statements) {
			super();
			this.name=name!=null ? name : UUID.randomUUID().toString();
			this.statements=statements!=null ? statements
				: new ArrayList<String>();
		}

		/**
		 *
		 *
		 */
		public String getName() {
			return name;
		}

		/**
		 *
		 *
		 */
		public List<String> getStatements() {
			return statements;
		}

		private String name;
		private List<String> statements;
	}


	/**
	 * 
	 * 
	 */
	public static class Builder {

		private Builder() {
			super();
			loader=new CQLLoader();
		}

		public Builder url(String value) {
			loader.setURL(value);
			return this;
		}

		public Builder stepNumber(int value) {
			loader.setCurrentStep(value);
			return this;
		}
		
		public Builder stepRange(int value) {
			loader.setStepRange(value);
			return this;
		}

		public Builder loadRelativeToClass(Class clazz) {
			loader.setLoadRelativeToClass(clazz);
			return this;
		}

		public Builder resourceRoot(String value) {
			loader.setResourceRoot(value);
			return this;
		}

		public Builder skipExecution(boolean value) {
			loader.setSkipExecution(value);
			return this;
		}
		
		public CQLLoader build()
				throws IOException {
			if (loader.getResourceRoot()!=null) {
				loader.loadSchema(null);
			}

			return loader;
		}
		
		private CQLLoader loader;
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields 
	////////////////////////////////////////////////////////////////////////////
	
	private static final int DEFAULT_RANGE=10000;
	private static final String DRIVER_CLASS_NAME=
		"org.apache.cassandra.cql.jdbc.CassandraDriver";
	private static final int UNSET_STEP=-1;

	private Connection connection;
	private Class<?> relativeToClass;
	private ClassLoader defaultClassLoader;
	private int stepRange=DEFAULT_RANGE;
	private int currentStep=0; //UNSET_STEP;
	private int numSteps;
	private boolean skipExecution;

	private String url;
	private ParsedCassandraURL parsedURL;
	private String resourceRoot;
	private AtomicReference<List<Step>> stepsRef=
		new AtomicReference<List<Step>>();
	private AtomicReference<List<Step>> revertStepsRef=
		new AtomicReference<List<Step>>();
	
	
	static {
		try {
			// Try to loadCQLFile the CQL driver
			Class.forName(DRIVER_CLASS_NAME);
		}
		catch (ClassNotFoundException e) {
			String MESSAGE="Could not load Cassandra CQL JDBC driver with "+
				"class "+DRIVER_CLASS_NAME;
			throw new RuntimeException(MESSAGE);
		}
	}
}
