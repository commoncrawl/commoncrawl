package org.commoncrawl.rpc.base.shared;

import java.io.IOException;

public class RPCException extends IOException {

	/**
   * 
   */
  private static final long serialVersionUID = -2660559646576235804L;

  public RPCException(Exception e) { 
		super(e);
	}

	public RPCException(String string) {
		super(string);
	}
}
