package org.commoncrawl.thriftrpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransport;
import org.commoncrawl.io.internal.NIOBufferList;
import org.commoncrawl.io.internal.NIOClientSocket;
import org.commoncrawl.io.internal.NIOClientSocketListener;
import org.commoncrawl.io.internal.NIOSocket;
import org.commoncrawl.util.shared.CCStringUtils;

/**
 * Class that implements a sort of state machine around the interaction with
 * a client and an invoker. It manages reading the frame size and frame data,
 * getting it handed off as wrapped transports, and then the writing of
 * reponse data back to the client. In the process it manages flipping the
 * read and write bits on the selection key for its client.
 */
class ThriftAsyncRemoteClientChannel implements NIOClientSocketListener {
  //
  // Possible states for the ThriftAsyncRemoteClientChannel state machine.
  //
  // in the midst of reading the frame size off the wire
  private static final int READING_FRAME_SIZE = 1;
  // reading the actual frame data now, but not all the way done yet
  private static final int READING_FRAME = 2;
  // completely read the frame, so an invocation can now happen
  private static final int READ_FRAME_COMPLETE = 3;


  
  //
  // Instance variables
  //

  // the actual transport hooked up to the client.
  public final TNonblockingTransport trans_;

  // the SelectionKey that corresponds to our transport
  private final SelectionKey selectionKey_;

  // where in the process of reading/writing are we?
  private int state_ = READING_FRAME_SIZE;

  // the ByteBuffer we'll be using to read frames into
  private ByteBuffer inputBuffer_;
  // output buffer list 
  NIOBufferList outputBuffer_ = new NIOBufferList();

  private TByteArrayOutputStream response_;
  
  ThriftAsyncServerChannel serverChannel_;

  public ThriftAsyncRemoteClientChannel(ThriftAsyncServerChannel serverChannel_, ThriftAsyncServerChannel serverChannel,final TNonblockingTransport trans,final SelectionKey selectionKey) {
    this.serverChannel_ = serverChannel_;
    serverChannel_ = serverChannel;
    trans_ = trans;
    selectionKey_ = selectionKey;
    inputBuffer_ = ByteBuffer.allocate(4);
  }

  /**
   * 
   * @return TProtocolFactory
   */
  TProtocolFactory getInputProtocolFactory() {
    return this.serverChannel_.inputProtocolFactory;
  }

  TProtocolFactory getOutputProtocolFactory() {
    return this.serverChannel_.outputProtocolFactory;
  }
  
  /**
   * Give this ThriftAsyncRemoteClientChannel a chance to read. The selector loop should have
   * received a read event for this ThriftAsyncRemoteClientChannel.
   *
   * @return true if the connection should live on, false if it should be
   * closed
   */
  public boolean read() {
    if (state_ == READING_FRAME_SIZE) {
      // try to read the frame size completely
      if (!internalRead()) {
        return false;
      }

      // if the frame size has been read completely, then prepare to read the
      // actual frame.
      if (inputBuffer_.remaining() == 0) {
        // pull out the frame size as an integer.
        int frameSize = inputBuffer_.getInt(0);
        if (frameSize <= 0) {
          ThriftAsyncServerChannel.LOG.error("Read an invalid frame size of " + frameSize
            + ". Are you using TFramedTransport on the client side?");
          return false;
        }

        // if this frame will always be too large for this server, log the
        // error and close the connection.
        if (frameSize > this.serverChannel_.MAX_READ_BUFFER_BYTES) {
          ThriftAsyncServerChannel.LOG.error("Read a frame size of " + frameSize
            + ", which is bigger than the maximum allowable buffer size for ALL connections.");
          return false;
        }

        // if this frame will push us over the memory limit, then return.
        // with luck, more memory will free up the next time around.
        if (this.serverChannel_.readBufferBytesAllocated + frameSize > this.serverChannel_.MAX_READ_BUFFER_BYTES) {
          return true;
        }

        // incremement the amount of memory allocated to read buffers
        this.serverChannel_.readBufferBytesAllocated += frameSize;

        // reallocate the readbuffer as a frame-sized buffer
        inputBuffer_ = ByteBuffer.allocate(frameSize);

        state_ = READING_FRAME;
      } else {
        // this skips the check of READING_FRAME state below, since we can't
        // possibly go on to that state if there's data left to be read at
        // this one.
        return true;
      }
    }

    // it is possible to fall through from the READING_FRAME_SIZE section
    // to READING_FRAME if there's already some frame data available once
    // READING_FRAME_SIZE is complete.

    if (state_ == READING_FRAME) {
      if (!internalRead()) {
        return false;
      }

      // since we're already in the select loop here for sure, we can just
      // modify our selection key directly.
      if (inputBuffer_.remaining() == 0) {
        // get rid of the read select interests
        selectionKey_.interestOps(0);
        state_ = READ_FRAME_COMPLETE;
      }

      return true;
    }

    // if we fall through to this point, then the state must be invalid.
    ThriftAsyncServerChannel.LOG.error("Read was called but state is invalid (" + state_ + ")");
    return false;
  }

  /**
   * Give this ThriftAsyncRemoteClientChannel a chance to write its output to the final client.
   */
  public boolean write() {
    if (outputBuffer_.available() != 0) {
      try {
        while (outputBuffer_.available() != 0) { 
          // get output buffer 
          ByteBuffer bufferToWrite = outputBuffer_.read();
          
          if (trans_.write(bufferToWrite) < 0) {
            return false;
          }
          else { 
            // ok see if we are done here ... 
            if (bufferToWrite.remaining() != 0) {
              outputBuffer_.putBack(bufferToWrite);
              // break out of loop
              break;
            }
          }
        }
      } catch (IOException e) {
        ThriftAsyncServerChannel.LOG.warn("Got an IOException during write!", e);
        return false;
      }
      if (outputBuffer_.available() == 0) { 
        // ok switch to read only selector mode 
        changeSelectInterests();
      }
      return true;
    }
    ThriftAsyncServerChannel.LOG.error("Write was called, but state is invalid (" + state_ + ")");
    return false;
  }

  /**
   * Give this ThriftAsyncRemoteClientChannel a chance to set its interest to write, once data
   * has come in.
   */
  public void changeSelectInterests() {
    if (outputBuffer_.available() != 0) {
      // set the OP_WRITE | OP_READ interest
      selectionKey_.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
    } else {
      selectionKey_.interestOps(SelectionKey.OP_READ);
    } 
  }

  /**
   * Shut the connection down.
   */
  public void close() {
    // if we're being closed due to an error, we might have allocated a
    // buffer that we need to subtract for our memory accounting.
    if (state_ == READING_FRAME || state_ == READ_FRAME_COMPLETE) {
      this.serverChannel_.readBufferBytesAllocated -= inputBuffer_.array().length;
    }
    trans_.close();
  }

  /**
   * Check if this ThriftAsyncRemoteClientChannel has a full frame read.
   */
  public boolean isFrameFullyRead() {
    return state_ == READ_FRAME_COMPLETE;
  }

  /**
   * After the processor has processed the invocation, whatever thread is
   * managing invocations should call this method on this ThriftAsyncRemoteClientChannel so we
   * know it's time to start trying to write again. Also, if it turns out
   * that there actually isn't any data in the response buffer, we'll skip
   * trying to write and instead go back to reading.
   */
  public synchronized void responseReady(NIOBufferList responseData) throws IOException {
    try {
      // take the incoming response data and merge it into the output list ...
      ByteBuffer bufferToTransfer = null;
      while ((bufferToTransfer = responseData.read()) != null) {
        // flip the buffer ... 
        bufferToTransfer.position(bufferToTransfer.limit());
        // and write it to the output buffer list 
        outputBuffer_.write(bufferToTransfer);
      }
      // do a final flush 
      outputBuffer_.flush();
      // request a change in the interest ops for the related selector key
      requestSelectInterestChange();
    }
    catch (IOException e) { 
      ThriftAsyncServerChannel.LOG.error(CCStringUtils.stringifyException(e));
      // ok close the underlying socket ... 
      close();
      // and bubble up the exception 
      throw e;
    }
  }

  /**
   * Actually invoke the method signified by this ThriftAsyncRemoteClientChannel.
   */
  public void invoke() throws TException {
    // initialize protocol ... 
    TProtocol inProt = this.serverChannel_.inputProtocolFactory.getProtocol(new TMemoryInputTransport(inputBuffer_.array()));
    // initialize incoming message (header)
    TMessage msg = inProt.readMessageBegin();
    // ok dispatch via processor
    this.serverChannel_.processor.process(this.serverChannel_, this,msg, inProt);
  }

  /**
   * Get the transport that should be used by the invoker for responding.
   */
  private TTransport getOutputTransport() {
    response_ = new TByteArrayOutputStream();
    return new TFramedTransport(new TIOStreamTransport(response_));
  }

  /**
   * Perform a read into buffer.
   *
   * @return true if the read succeeded, false if there was an error or the
   * connection closed.
   */
  private boolean internalRead() {
    try {
      if (trans_.read(inputBuffer_) < 0) {
        return false;
      }
      return true;
    } catch (IOException e) {
      ThriftAsyncServerChannel.LOG.warn("Got an IOException in internalRead!", e);
      return false;
    }
  }

  /**
   * We're done writing, so reset our interest ops and change state accordingly.
   */
  void prepareRead() {
    // get ready for another go-around
    inputBuffer_ = ByteBuffer.allocate(4);
    state_ = READING_FRAME_SIZE;
  }

  /**
   * When this ThriftAsyncRemoteClientChannel needs to change it's select interests and execution
   * might not be in the select thread, then this method will make sure the
   * interest change gets done when the select thread wakes back up. When the
   * current thread is the select thread, then it just does the interest change
   * immediately.
   */
  private void requestSelectInterestChange() {
    if (Thread.currentThread() == this.serverChannel_.eventLoop.getEventThread()) {
      changeSelectInterests();
    } else {
      this.serverChannel_.requestSelectInterestChange(this);
    }
  }

  @Override
  public void Connected(NIOClientSocket theSocket) throws IOException {
    // NOOP for a incoming connection ... 
  }

  @Override
  public int Readable(NIOClientSocket theSocket) throws IOException {
    // NIOSocketSelector protocol expects Readble to return 
    // -1 if read failed or 0 or more bytes read.
    // we emulate this by checking the result of read()
    // if false, it means read failed and we return -1
    // else always return 0 instead of actual bytes read(acceptable response).

    // if the buffer's frame read is complete, invoke the method.
    if (!read()) {
      return -1;
    }
    else { 
      if (isFrameFullyRead()) {
        try {
          if (!this.serverChannel_.requestInvoke(this)) {
            return -1;
          }
        } catch (TException e) {
          ThriftAsyncServerChannel.LOG.error(CCStringUtils.stringifyException(e));
          return -1;
        }
      }
    }
    return 0;
  }

  @Override
  public void Writeable(NIOClientSocket theSocket) throws IOException {
    if (!write()) {
      close();
    }
  }

  @Override
  public void Disconnected(NIOSocket theSocket, Exception optionalException)
      throws IOException {
    // we received a potential abnormal disconnect... 
    // close the channel 
    close();
  }

  @Override
  public void Excepted(NIOSocket socket, Exception e) {
    ThriftAsyncServerChannel.LOG.error(CCStringUtils.stringifyException(e));
    close();
  }

}