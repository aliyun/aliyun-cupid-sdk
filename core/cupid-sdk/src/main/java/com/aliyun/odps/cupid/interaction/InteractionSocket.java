/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * 
 */
package com.aliyun.odps.cupid.interaction;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.cupid.utils.SDKConstants;

/**
 * InteractionSocket WebSocket implementation
 * 
 * @author emerson
 *
 */
@WebSocket(maxBinaryMessageSize = SDKConstants.WEBSOCKET_MAX_BINARY_MESSAGE_SIZE,
    maxIdleTime = SDKConstants.ONE_HOUR)
public class InteractionSocket {

  private static final Logger LOG = LoggerFactory.getLogger(InteractionSocket.class);

  private InputStream inputStream;

  private OutputStream outputStream;

  private ByteBuffer inputBuf;

  private ByteBuffer outputBuf;

  private FileOutputStream fileOutputStream;

  private volatile boolean closed;

  private WebSocketClient webSocketClient;

  private int mode;

  public InteractionSocket(WebSocketClient webSocketClient, int inputMode) {
    this.webSocketClient = webSocketClient;
    this.mode = inputMode;
  }

  @OnWebSocketClose
  public void onClose(int statusCode, String reason) {
    LOG.info(webSocketClient.getSubProtocol() + " - Connection closed: " + statusCode + " - "
        + reason);
    if (confirmClose(statusCode, reason)) {
      webSocketClient.close();
    } else {
      LOG.info(webSocketClient.getSubProtocol() + " - Try to reconnect...");
      webSocketClient.connect(true);
    }
  }

  public void close() {
    closed = true;
    if (SDKConstants.INTERACTION_CLIENT_INPUT_MODE_INPUTSTREAM == mode) {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
        }
      }
    } else {
      if (fileOutputStream != null) {
        try {
          fileOutputStream.close();
        } catch (IOException e) {
        }
      }
    }
    if (outputStream != null) {
      try {
        outputStream.close();
      } catch (IOException e) {
      }
    }
    if (inputBuf != null) {
      synchronized (inputBuf) {
        inputBuf.notifyAll();
      }
    }
  }

  public boolean isClosed() {
    return closed;
  }

  @OnWebSocketError
  public void onError(Session session, Throwable errors) {
    LOG.error(webSocketClient.getSubProtocol() + " - socket error", errors);
  }

  @OnWebSocketConnect
  public void onConnect(Session session) {
    LOG.info(webSocketClient.getSubProtocol() + " - Got connect.");
    if (mode == SDKConstants.INTERACTION_CLIENT_INPUT_MODE_INPUTSTREAM) {
      if (inputBuf == null) {
        this.inputBuf = ByteBuffer.allocate(SDKConstants.WEBSOCKET_MAX_BINARY_MESSAGE_SIZE);
        this.inputBuf.limit(0);
        this.inputStream = new ByteBufferBackedInputStream(inputBuf);
      }
    }
    if (outputBuf == null) {
      this.outputBuf = ByteBuffer.allocate(SDKConstants.WEBSOCKET_MAX_BINARY_MESSAGE_SIZE);
      this.outputStream = new ByteBufferBackedOutputStream(outputBuf, session);
    } else {
      synchronized (outputBuf) {
        ((ByteBufferBackedOutputStream) this.outputStream).setSession(session);
        outputBuf.notify();
      }
    }
    this.closed = false;
  }

  @OnWebSocketMessage
  public synchronized void onMessage(Session session, byte buf[], int offset, int length) {
    if (mode == SDKConstants.INTERACTION_CLIENT_INPUT_MODE_INPUTSTREAM) {
      synchronized (inputBuf) {
        inputBuf.clear();
        inputBuf.put(buf, offset, length);
        inputBuf.flip();
        while (inputBuf.hasRemaining()) {
          inputBuf.notify();
          if (closed) {
            return;
          }
          try {
            inputBuf.wait(1000);
          } catch (InterruptedException e) {
          }
        }
        this.inputBuf.limit(0);
      }
    } else {
      try {
        fileOutputStream.write(buf, offset, length);
      } catch (IOException e) {
        throw new RuntimeException("Write into inputStream failed!", e);
      }
    }
  }

  public void setInput(FileDescriptor fd) {
    if (SDKConstants.INTERACTION_CLIENT_INPUT_MODE_FD == mode) {
      if (fileOutputStream == null) {
        fileOutputStream = new FileOutputStream(fd);
      } else {
        throw new RuntimeException("The fd was set and could't be overwritten!");
      }
    } else if (SDKConstants.INTERACTION_CLIENT_INPUT_MODE_INPUTSTREAM == mode) {
      throw new RuntimeException(
          "The mode cannot be changed! Now is inputStream mode and the fd cannot be set!");
    } else {
      throw new RuntimeException("Invalid mode!");
    }
  }

  private boolean confirmClose(int statusCode, String reason) {
    return statusCode == StatusCode.NORMAL
        || SDKConstants.WEBSOCKET_STATUS_MESSAGE_SHUTDOWN.equalsIgnoreCase(reason);
  }

  public InputStream getInputStream() {
    if (SDKConstants.INTERACTION_CLIENT_INPUT_MODE_INPUTSTREAM == mode) {
      return inputStream;
    } else if (SDKConstants.INTERACTION_CLIENT_INPUT_MODE_FD == mode) {
      throw new RuntimeException(
          "The mode cannot be changed! Now is fd mode, you cannot get inputStream!");
    } else {
      throw new RuntimeException("Invalid mode!");
    }
  }

  public OutputStream getOutputStream() {
    return outputStream;
  }

  class ByteBufferBackedInputStream extends InputStream {

    ByteBuffer buf;

    boolean closed;

    ByteBufferBackedInputStream(ByteBuffer buf) {
      this.buf = buf;
      this.closed = false;
    }

    public int read() throws IOException {
      checkClose();
      synchronized (buf) {
        while (!buf.hasRemaining()) {
          buf.notify();
          if (closed) {
            return -1;
          }
          try {
            buf.wait(1000);
          } catch (InterruptedException e) {
          }
        }
        return buf.get();
      }
    }

    public int read(byte[] bytes, int off, int len) throws IOException {
      checkClose();
      synchronized (buf) {
        while (!buf.hasRemaining()) {
          buf.notify();
          if (closed) {
            return -1;
          }
          try {
            buf.wait(1000);
          } catch (InterruptedException e) {
          }
        }
        len = Math.min(len, buf.remaining());
        buf.get(bytes, off, len);
        return len;
      }
    }

    @Override
    public void close() throws IOException {
      closed = true;
      if (buf != null) {
        synchronized (buf) {
          buf.notifyAll();
        }
      }
    }

    private void checkClose() throws IOException {
      if (closed) {
        throw new IOException(webSocketClient.getSubProtocol() + " - The stream is closed.");
      }
    }


  }

  class ByteBufferBackedOutputStream extends OutputStream {

    ByteBuffer buf;
    boolean closed;
    Session session;
    long lastFlushTime;


    ByteBufferBackedOutputStream(ByteBuffer buf, Session session) {
      this.buf = buf;
      this.closed = false;
      this.session = session;
      this.lastFlushTime = System.currentTimeMillis();
    }

    public void write(int b) throws IOException {
      checkClose();
      synchronized (buf) {
        buf.put((byte) b);
        buf.flip();
        buf.mark();
        sendData();
        buf.clear();
        lastFlushTime = System.currentTimeMillis();
        return;

      }
    }

    public void write(byte[] bytes, int off, int len) throws IOException {
      checkClose();
      synchronized (buf) {
        buf.flip();
        buf.mark();
        if (buf.hasRemaining()) {
          sendData();
        }
        buf.clear();
        buf.put(bytes, off, len);
        buf.flip();
        buf.mark();
        sendData();
        buf.clear();
        lastFlushTime = System.currentTimeMillis();
      }
    }

    private void sendData() {
      while (!closed) {
        try {
          session.getRemote().sendBytes(buf);
          return;
        } catch (Exception e) {
          try {
            buf.wait(2000);
          } catch (InterruptedException e1) {
          }
        }
      }
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }

    private void checkClose() throws IOException {
      if (closed) {
        throw new IOException(webSocketClient.getSubProtocol() + " - The stream is closed.");
      }
    }

    public void setSession(Session session) {
      this.session = session;
    }

  }

}
