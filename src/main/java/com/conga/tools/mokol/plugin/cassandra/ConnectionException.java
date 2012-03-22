package com.conga.tools.mokol.plugin.cassandra;

import com.conga.tools.mokol.ShellException;

/**
 *
 * @author Todd Fast
 */
public class ConnectionException extends ShellException {

	public ConnectionException() {
		super();
	}

	public ConnectionException(String message) {
		super(message);
	}

	public ConnectionException(Throwable rootCause) {
		super(rootCause);
	}

	public ConnectionException(String message, Throwable rootCause) {
		super(message,rootCause);
	}
}
