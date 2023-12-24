const express = require('express');
const { createServer } = require('node:http');
const { join } = require('node:path');
const { Server } = require('socket.io');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const { availableParallelism } = require('node:os');
const cluster = require('node:cluster');
const { createAdapter, setupPrimary } = require('@socket.io/cluster-adapter');

if (cluster.isPrimary) {
  const numCPUs = availableParallelism();
  // create one worker per available core
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({
      PORT: 8080 + i,
    });
  }

  // set up the adapter on the primary thread
  return setupPrimary();
}

async function ChatBot() {
  const db = await open({
    filename: 'chat.db',
    driver: sqlite3.Database,
  });

  await db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_offset TEXT UNIQUE,
        content TEXT
    );
  `);

  const app = express();
  const server = createServer(app);

  const io = new Server(server, {
    connectionStateRecovery: {},
    adapter: createAdapter(),
  });

  app.get('/', (req, res) => {
    res.sendFile(join(__dirname, 'index.html'));
  });

  io.on('connection', async (socket) => {
    console.log('User Connected');

    socket.on('chat message', async (msg, clientOffset, callback) => {
      let result;
      try {
        result = await db.run(
          'INSERT INTO messages (content, client_offset) VALUES (?, ?)',
          msg,
          clientOffset
        );
      } catch (e) {
        console.log('Writing to DB failed', e);
        if (e.errno === 19 /* SQLITE_CONSTRAINT */) {
          // the message was already inserted, so we notify the client
          callback();
        } else {
          // nothing to do, just let the client retry
        }

        return;
      }
      // include the offset with the message
      socket.broadcast.emit('chat message', msg, result.lastID);
      // acknowledge the event
      callback();
    });

    socket.on('disconnect', () => {
      console.log('User Disconnected');
    });

    socket.onAny((eventName, ...args) => {
      console.log('INCOMING - Event Name: ', eventName);
      console.log('INCOMING - Arguments: ', args);
    });

    socket.onAnyOutgoing((eventName, ...args) => {
      console.log('OUT - Event Name: ', eventName);
      console.log('OUT - Arguments: ', args);
    });

    if (!socket.recovered) {
      // if the connection state recovery was not successful
      try {
        await db.each(
          'SELECT id, content FROM messages WHERE id > ?',
          [socket.handshake.auth.serverOffset || 0],
          (_err, row) => {
            socket.emit('chat message', row.content, row.id);
          }
        );
      } catch (e) {
        console.log('Socket recovery error: ', e);
      }
    }
  });

  const port = process.env.PORT;

  server.listen(port, () => {
    console.log(`server running at http://localhost:${port}`);
  });
}

ChatBot();

module.exports = { ChatBot };
