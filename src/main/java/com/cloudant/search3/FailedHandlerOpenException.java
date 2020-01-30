package com.cloudant.search3;

import java.io.IOException;

public class FailedHandlerOpenException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public FailedHandlerOpenException(final IOException e) {
    super(e);
  }
}
