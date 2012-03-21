package com.conga.tools.mokol.plugin.cassandra.cql;

/**
 *
 *
 * @author Todd Fast
 */
/*pkg*/ class ColumnMetadata {

	public ColumnMetadata(String s1, String s2, String s3) {
		super();
		this.s1=s1;
		this.s2=s2;
		this.s3=s3;
	}

	public String get1() {
		return s1;
	}

	public String get2() {
		return s2;
	}

	public String get3() {
		return s3;
	}




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	private String s1;
	private String s2;
	private String s3;
}
