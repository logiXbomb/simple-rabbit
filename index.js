const amqp = require('amqplib');

let connection;
const rabbit = {
  init(callback = () => {}, uri = process.env.RABBIT_URI || 'rabbit', attempts = 5) {
    if (connection) return callback(connection);
    amqp.connect(`amqp://${uri}`)
    .then(conn => {
      connection = conn;
      callback(connection)
    })
    .catch(error => {
      attempts > 0 ? setTimeout(() => { this.init(callback, uri, attempts--); }, 2500) : console.log(error);
    });
  },
  send(q, msg) {
    this.init(conn => {
      conn.createChannel().then(ch => ch.assertQueue(q)
        .then(() => ch.sendToQueue(q, new Buffer(JSON.stringify(msg)))));
    });
  },
  receive(q, callback) {
    this.init(conn => {
      conn.createChannel().then(ch => ch.assertQueue(q)
        .then(() => ch.consume(q, msg => {
          let json = msg.content.toString();
          try {
            json = JSON.parse(json);
          } catch (err) {
            console.log(err);
          }
          try {
            json = JSON.parse(json);
          } catch (err) {
            if (!/Unexpected token .* in JSON at position .*/.test(err.message)) {
              console.log(err);
            }
          }
          callback(json);
          ch.ack(msg);
        })));
    });
  }
}

module.exports = rabbit;
