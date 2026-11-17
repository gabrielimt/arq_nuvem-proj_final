const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Kafka } = require('kafkajs');
const client = require('prom-client');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// Configuração do PostgreSQL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://chatuser:chatpassword@localhost:5432/chatdb'
});

async function initDB() {
  let retries = 10;
  while (retries > 0) {
    try {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS user_logins (
          id SERIAL PRIMARY KEY,
          username VARCHAR(100) NOT NULL,
          login_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
      `);
      console.log('Tabela user_logins verificada/criada com sucesso no PostgreSQL.');
      break;
    } catch (err) {
      retries--;
      console.log(`Aguardando PostgreSQL iniciar... Tentativas restantes: ${retries}`);
      await new Promise(res => setTimeout(res, 3000));
    }
  }
}

// Configuração do Prometheus Metrics
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const messagesSentCounter = new client.Counter({
  name: 'chat_messages_sent_total',
  help: 'Total de mensagens enviadas ao Kafka pelo chat',
  registers: [register]
});

const messagesReceivedCounter = new client.Counter({
  name: 'chat_messages_received_total',
  help: 'Total de mensagens consumidas do Kafka e entregues aos clientes',
  registers: [register]
});

const activeUsersGauge = new client.Gauge({
  name: 'chat_active_users',
  help: 'Número de usuários conectados via WebSocket atualmente',
  registers: [register]
});

app.use(express.static('public'));

// Endpoint para consultar logins registrados no PostgreSQL
app.get('/api/logins', async (req, res) => {
  try {
    const result = await pool.query('SELECT id, username, login_time FROM user_logins ORDER BY login_time DESC LIMIT 50');
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: 'Erro ao consultar logins no banco de dados' });
  }
});

// Endpoint de métricas para o Prometheus
app.get('/metrics', async (req, res) => {
  res.setHeader('Content-Type', register.contentType);
  res.send(await register.metrics());
});

// Configuração do Kafka
const kafka = new Kafka({
  clientId: 'chat-app',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'chat-group' });
const TOPIC = 'chat-messages';

async function runKafka() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const parsedMessage = JSON.parse(message.value.toString());
      messagesReceivedCounter.inc();
      io.emit('chat-message', parsedMessage);
    },
  });
}

io.on('connection', (socket) => {
  activeUsersGauge.inc();
  console.log('Um usuário se conectou.');

  socket.on('disconnect', () => {
    activeUsersGauge.dec();
    console.log('Um usuário se desconectou.');
  });

  // Registra o login do usuário no PostgreSQL na primeira identificação
  socket.on('user-login', async (username) => {
    if (!socket.hasLoggedIn && username && username.trim() !== '') {
      socket.hasLoggedIn = true;
      try {
        await pool.query(
          'INSERT INTO user_logins (username, login_time) VALUES ($1, NOW())',
          [username.trim()]
        );
        console.log(`Login registrado no PostgreSQL: ${username.trim()}`);
      } catch (err) {
        console.error('Erro ao salvar login no PostgreSQL:', err);
      }
    }
  });

  socket.on('chat-message', async (data) => {
    if (!socket.hasLoggedIn && data.user && data.user.trim() !== '') {
      socket.hasLoggedIn = true;
      try {
        await pool.query(
          'INSERT INTO user_logins (username, login_time) VALUES ($1, NOW())',
          [data.user.trim()]
        );
        console.log(`Login registrado no PostgreSQL: ${data.user.trim()}`);
      } catch (err) {
        console.error('Erro ao salvar login no PostgreSQL:', err);
      }
    }

    await producer.send({
      topic: TOPIC,
      messages: [{ value: JSON.stringify(data) }],
    });
    messagesSentCounter.inc();
  });
});

initDB()
  .then(() => runKafka())
  .catch(console.error);

server.listen(3000, () => {
  console.log('Servidor rodando na porta 3000');
});
