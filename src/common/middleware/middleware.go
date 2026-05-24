package middleware

import "errors"

var (
	ErrMessageMiddlewareMessage      = errors.New("message middleware: message error")
	ErrMessageMiddlewareDisconnected = errors.New("message middleware: disconnected")
	ErrMessageMiddlewareClose        = errors.New("message middleware: close error")
)

// PrefetchCount caps how many unacknowledged AMQP messages the broker may push
// to a single consumer. With batches of ~64 KB, 64 keeps ~4 MB unacked per
// consumer — the broker's unacked-tracking stays cheap and round-robin between
// replicas remains responsive.
const PrefetchCount = 64

type Message struct {
	Body string
}

type ConnSettings struct {
	Hostname string
	Port     int
}

type Middleware interface {

	//Comienza a escuchar a la cola/exchange e invoca a callbackFunc tras
	//cada mensaje de datos o de control con el cuerpo del mensaje.
	//callbackFunc tiene como parámetro:
	// msg - El struct tal y como lo recibe el método Send.
	// ack - Una función que hace ACK del mensaje recibido.
	// nack - Una función que hace NACK del mensaje recibido.
	//Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
	//Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
	StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error

	//Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	//no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	//Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
	StopConsuming() error

	//Envía un mensaje a la cola o a los tópicos con el que se inicializó el exchange.
	//Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
	//Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
	Send(msg Message) error

	//SendWithKey publica un mensaje a un exchange direct con la routing key
	//indicada (override de la routing key fija del constructor). En middlewares
	//que no soportan routing keys (queue middleware) se delega a Send.
	SendWithKey(msg Message, routingKey string) error

	//Se desconecta de la cola o exchange al que estaba conectado.
	//Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareClose.
	Close() error
}
