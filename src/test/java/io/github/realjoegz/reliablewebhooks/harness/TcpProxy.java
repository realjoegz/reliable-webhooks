package io.github.realjoegz.reliablewebhooks.harness;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A TCP forwarder inside the test JVM, between one replica and the database. It can make the
 * connections of that replica half-open, which is what a failover or a partition leaves behind:
 * both ends still have their socket, and no byte and no close arrives at either any more.
 */
public final class TcpProxy implements AutoCloseable {

  private static final byte[] LOOPBACK = {127, 0, 0, 1};

  /** A connection of the replica together with the connection to the database that carries it. */
  private static final class Relay {

    private final Socket replica;
    private final Socket database;

    // A byte is relayed under the read lock and the relay is cut under the write lock. No byte
    // is therefore relayed once the cut has returned.
    private final ReadWriteLock relaying = new ReentrantReadWriteLock();
    private boolean cut;

    private Relay(Socket replica, Socket database) {
      this.replica = replica;
      this.database = database;
    }

    private void cut() {
      relaying.writeLock().lock();
      try {
        cut = true;
      } finally {
        relaying.writeLock().unlock();
      }
    }

    // What one end sends goes to the other, and so does the end of its stream. After the cut
    // neither does: the bytes are dropped, and an end that closes leaves the other end open.
    private void pump(Socket from, Socket to) {
      try {
        InputStream in = from.getInputStream();
        OutputStream out = to.getOutputStream();
        byte[] buffer = new byte[16 * 1024];
        for (int read = in.read(buffer); read != -1; read = in.read(buffer)) {
          if (!relayed(out, buffer, read)) {
            return;
          }
        }
      } catch (IOException connectionGone) {
        // Handled like the end of the stream.
      }
      closeUnlessCut();
    }

    private boolean relayed(OutputStream out, byte[] bytes, int length) throws IOException {
      relaying.readLock().lock();
      try {
        if (cut) {
          return false;
        }
        out.write(bytes, 0, length);
        out.flush();
        return true;
      } finally {
        relaying.readLock().unlock();
      }
    }

    private void closeUnlessCut() {
      relaying.readLock().lock();
      try {
        if (!cut) {
          close();
        }
      } finally {
        relaying.readLock().unlock();
      }
    }

    private void close() {
      closeQuietly(replica);
      closeQuietly(database);
    }
  }

  private final ServerSocket server;
  private final String databaseHost;
  private final int databasePort;
  private final ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor();
  private final List<Relay> relays = new ArrayList<>();
  private final List<Socket> ignored = new ArrayList<>();
  private boolean blackholed;

  private TcpProxy(ServerSocket server, String databaseHost, int databasePort) {
    this.server = server;
    this.databaseHost = databaseHost;
    this.databasePort = databasePort;
  }

  /** A proxy on a free port of the loopback address that relays to {@link TestDatabase}. */
  public static TcpProxy toDatabase() throws IOException {
    TcpProxy proxy =
        new TcpProxy(
            new ServerSocket(0, 0, InetAddress.getByAddress(LOOPBACK)),
            TestDatabase.host(),
            TestDatabase.port());
    proxy.threads.execute(proxy::accept);
    return proxy;
  }

  public int port() {
    return server.getLocalPort();
  }

  /**
   * Keeps every connection that exists open and relays nothing more on it, ever. A connection that
   * is opened from now on is accepted and gets no answer.
   */
  public synchronized void blackhole() {
    blackholed = true;
    relays.forEach(Relay::cut);
  }

  /**
   * Lets connections that are opened from now on relay again. Those that a blackhole cut stay cut.
   */
  public synchronized void heal() {
    blackholed = false;
  }

  @Override
  public synchronized void close() {
    closeQuietly(server);
    relays.forEach(Relay::close);
    ignored.forEach(TcpProxy::closeQuietly);
    threads.shutdownNow();
  }

  private void accept() {
    while (true) {
      Socket replica;
      try {
        replica = server.accept();
      } catch (IOException closed) {
        return;
      }
      connect(replica);
    }
  }

  // Under the lock of blackhole(), so a connection is either relayed and then cut by it, or
  // ignored because of it.
  private synchronized void connect(Socket replica) {
    if (blackholed) {
      ignored.add(replica);
      return;
    }
    try {
      Socket database = new Socket(databaseHost, databasePort);
      // The driver waits for the answer to each small message, which a delayed segment slows.
      replica.setTcpNoDelay(true);
      database.setTcpNoDelay(true);
      Relay relay = new Relay(replica, database);
      relays.add(relay);
      threads.execute(() -> relay.pump(replica, database));
      threads.execute(() -> relay.pump(database, replica));
    } catch (IOException databaseNotReachable) {
      closeQuietly(replica);
    }
  }

  private static void closeQuietly(AutoCloseable socket) {
    try {
      socket.close();
    } catch (Exception alreadyGone) {
      // Nothing is left to close.
    }
  }
}
