package com.conga.tools.mokol.plugin.cassandra;

/**
 * Parses a Cassandra JDBC URL into its component parts
 *
 * @author Todd Fast
 */
public class ParsedCassandraURL {

	/**
	 * 
	 * 
	 */
	public ParsedCassandraURL(String url) {
		super();
		parse(url);
	}


	/**
	 *
	 *
	 */
	protected void parse(String url) {
		// Format:
		// [[jdbc:cassandra:]<user>/<password>@]<server>:<port>/<keyspace>

		final String JDBC_PREFIX="jdbc:cassandra:";
		if (url.startsWith(JDBC_PREFIX))
			url=url.substring(JDBC_PREFIX.length());

		// [<user>/<password>@]<server>:<port>/<keyspace>

		String[] split;
		int index=-1;
		if ((index=url.indexOf("@"))!=-1) {
			String userAndPassword=url.substring(0,index);

			// Strip the user/password
			url=url.substring(index+1);

			split=userAndPassword.split("/");

			user=split[0];
			password=split[1];
		}

		// <server>:<port>/<keyspace>
		split=url.split(":");

		server=split[0];

		split=split[1].split("/");
		port=Integer.parseInt(split[0]);
		keyspace=split[1];
	}


	/**
	 *
	 *
	 */
	public String getURL() {
		return url;
	}


	/**
	 *
	 *
	 */
	public void setURL(String url) {
		this.url=url;
	}


	/**
	 *
	 *
	 */
	public String getUser() {
		return user;
	}


	/**
	 *
	 *
	 */
	public void setUser(String user) {
		this.user=user;
	}


	/**
	 *
	 *
	 */
	public String getPassword() {
		return password;
	}


	/**
	 *
	 *
	 */
	public void setPassword(String password) {
		this.password=password;
	}


	/**
	 *
	 *
	 */
	public String getServer() {
		return server;
	}


	/**
	 *
	 *
	 */
	public void setServer(String server) {
		this.server=server;
	}


	/**
	 *
	 *
	 */
	public int getPort() {
		return port;
	}


	/**
	 *
	 *
	 */
	public void setPort(int port) {
		this.port=port;
	}


	/**
	 *
	 *
	 */
	public String getKeyspace() {
		return keyspace;
	}


	/**
	 *
	 *
	 */
	public void setKeyspace(String keyspace) {
		this.keyspace=keyspace;
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private String url;
	private String user;
	private String password;
	private String server;
	private int port;
	private String keyspace;
}
