package pq

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/armon/relay"
	"github.com/armon/relay/broker"
)

// The pq package provides a simple priority queue. All that is required is a
// a known number of available priorities. Queues labeled from 0..N will be
// automatically created, where the higher numbers are higher priority queues.
// High-level methods are exposed to make dealing with the priority queues
// very simple and abstract.

var (
	// The duration of a read attempt from an individual AMQP queue. The
	// consumer methods use this value to run short Consume() calls so that
	// we can check for interrupts. The higher this number is, the longer it
	// it may take to interrupt a read and exit.
	consumeResetInterval = 500 * time.Millisecond

	// The minimum amount of time to wait after receiving a message for a higher
	// priority message to arrive.
	MinQuietPeriod = 10 * time.Millisecond
)

// PriorityQueue is a simple wrapper around a relay.Broker to manage
// a set of queues with varying priority.
type PriorityQueue struct {
	max          int
	source       broker.Broker
	prefix       string
	quietPeriod  time.Duration
	publishers   []broker.Publisher
	shutdownLock sync.Mutex
	shutdown     bool
}

// priorityResp is used as a container for response data during a Consume().
// Since the response must be fed down a channel and contains multiple values,
// we stuff them all into this struct and thread it through.
type priorityResp struct {
	value    interface{}
	priority int
	consumer broker.Consumer
}

// NewPriorityQueue returns a new priority queue from which a consumer or
// producer at a given priority can be easily retrieved.
func NewPriorityQueue(
	b broker.Broker,
	pri int,
	prefix string,
	quietPeriod time.Duration) (*PriorityQueue, error) {

	if b == nil {
		return nil, fmt.Errorf("Broker must not be nil")
	}
	if pri < 1 {
		return nil, fmt.Errorf("Must be 1 or more priorities")
	}

	// Guard against a quiet period which is too small
	if quietPeriod < MinQuietPeriod {
		quietPeriod = MinQuietPeriod
	}

	q := PriorityQueue{
		source:      b,
		prefix:      prefix,
		max:         pri - 1,
		quietPeriod: quietPeriod,
	}

	// Initialize the publisher cache
	q.publishers = make([]broker.Publisher, pri)

	return &q, nil
}

// queueName formats the name of a priority queue by appending its priority to
// the user-provided queue prefix.
func queueName(prefix string, pri int) string {
	return fmt.Sprintf("%s-%d", prefix, pri)
}

// Max returns the highest priority number.
func (q *PriorityQueue) Max() int {
	return q.max
}

// Min returns the lowest priority number. This is always 0.
func (q *PriorityQueue) Min() int {
	return 0
}

// publisher returns a new publisher from the priority indicated by pri.
func (q *PriorityQueue) publisher(pri int) (broker.Publisher, error) {
	if pri > q.Max() || pri < q.Min() {
		return nil, fmt.Errorf("Priority out of range: %d", pri)
	}

	if q.publishers[pri] == nil {
		pub, err := q.source.Publisher(queueName(q.prefix, pri))
		if err != nil {
			return nil, err
		}
		q.publishers[pri] = pub
	}

	return q.publishers[pri], nil
}

// consumer returns a new consumer with the indicated priority.
func (q *PriorityQueue) consumer(pri int) (broker.Consumer, error) {
	if pri > q.Max() || pri < q.Min() {
		return nil, fmt.Errorf("Priority out of range: %d", pri)
	}

	cons, err := q.source.Consumer(queueName(q.prefix, pri))
	if err != nil {
		return nil, err
	}

	return cons, nil
}

// Publish will publish a message at a given priority. The publisher is
// automatically closed afterward.
func (q *PriorityQueue) Publish(payload interface{}, pri int) error {
	pub, err := q.publisher(pri)
	if err != nil {
		return err
	}

	if err := pub.Publish(payload); err != nil {
		return err
	}

	return nil
}

// consume consumes a message from the priority queue. This is a blocking call
// which will watch every queue at every priority until a message is received on
// at least one of them. Once a message is received, we continue blocking for a
// configurable amount of time for any other higher priority message to arrive.
// After the quiet period, if no higher priority message has been received, all
// lower priority messages are marked for re-delivery, and the most urgent
// message is returned.
//
// The quiet period should almost always carry a value greater than zero. If
// there is no quiet period, then messages are returned as soon as they are
// received, which makes the quickness of the server and queue the decider of
// message priority. Therefore, we enforce a minimum quiet period.
//
// The consumer is also returned as part of the result, as it will contain
// the session open to the queue with an un-Ack()'ed message. It is the
// responsibility of the caller to Ack() and Close() the consumer.
func (q *PriorityQueue) consume(
	out interface{}, cancelCh chan struct{}) (broker.Consumer, int, error) {

	shutdownCh := make(chan struct{})

	// Populate the cancelCh if none was provided
	if cancelCh == nil {
		cancelCh = make(chan struct{})
	}

	// Initialize the data channels
	errCh := make(chan error, q.Max()+1)
	respCh := make(chan priorityResp, q.Max()+1)

	var consumers []broker.Consumer
	for i := q.Min(); i <= q.Max(); i++ {
		cons, err := q.consumer(i)
		if err != nil {
			return cons, 0, err
		}
		consumers = append(consumers, cons)
	}

	for i := q.Min(); i <= q.Max(); i++ {
		go func(cons broker.Consumer, pri int) {
			val := reflect.New(reflect.TypeOf(out)).Interface()
			for {
				select {
				case <-cancelCh:
					cons.Close()
					return
				case <-shutdownCh:
					cons.Close()
					return
				default:
				}

				// Keep consuming with timeout so we can bail if we need to
				if err := cons.ConsumeTimeout(&val, consumeResetInterval); err != nil {
					if err == relay.TimedOut {
						continue
					}
					errCh <- err
					return
				}

				// Check for cancellation
				select {
				case <-cancelCh:
					if cons != nil {
						cons.Nack()
						cons.Close()
					}
				default:
				}

				// Create the response object to send down the channel
				r := priorityResp{
					value:    val,
					priority: pri,
					consumer: cons,
				}
				respCh <- r
				return
			}
		}(consumers[i], i)
	}

	var wait <-chan time.Time
	var cons broker.Consumer
	highest := q.Min() - 1 // Allows Min() messages to be accepted

OUTER:
	for {
		select {
		case err := <-errCh:
			close(shutdownCh)
			return cons, 0, err

		case r := <-respCh:
			if r.priority <= highest {
				// Send a negative acknowledgement and close the consumer
				r.consumer.Nack()
				r.consumer.Close()
				continue
			}

			// Close the outstanding consumer. This is only set if some other
			// value had previously been considered the highest value.
			if cons != nil {
				cons.Nack()
				cons.Close()
			}

			// Received message was higher priority, so re-assign the results
			// and enter the quiet period for any higher-priority messages
			// to arrive.
			dst := reflect.Indirect(reflect.ValueOf(out))
			src := reflect.Indirect(reflect.ValueOf(r.value))
			dst.Set(reflect.Indirect(src))

			highest = r.priority
			cons = r.consumer
			wait = time.After(q.quietPeriod)

		case <-wait:
			// Signal all higher-priority readers to close, they didn't receive
			// any messages we can use.
			close(shutdownCh)
			return cons, highest, nil

		case <-cancelCh:
			break OUTER

		case <-shutdownCh:
			break OUTER
		}
	}

	return cons, 0, nil
}

// Consume is the public method for consuming data out of a priority queue. It
// will block until data is received, and returns the priority level of the
// consumed message along with any errors. The consumer is also returned, which
// should be Ack'ed and Closed by the caller.
func (q *PriorityQueue) Consume(out interface{}) (broker.Consumer, int, error) {
	return q.consume(out, nil)
}

// ConsumeCancel allows passing in a channel to signal that we should
// stop trying to consume a message. Internally this channel will be checked on
// a short interval, and will shut down the job if the channel has been closed.
func (q *PriorityQueue) ConsumeCancel(
	out interface{}, cancelCh chan struct{}) (broker.Consumer, int, error) {

	if cancelCh == nil {
		return nil, 0, fmt.Errorf("Cancellation channel cannot be nil")
	}
	return q.consume(out, cancelCh)
}

// Close will call a shutdown on all publishers we have used. By default, all
// publishers are kept open so that multiple calls to establish the sessions are
// not always required. This method shuts them all down and resets the pool.
func (q *PriorityQueue) Close() error {
	q.shutdownLock.Lock()
	defer q.shutdownLock.Unlock()

	if q.shutdown {
		return nil
	}
	q.shutdown = true

	for i, pub := range q.publishers {
		if pub != nil {
			if err := pub.Close(); err != nil {
				return err
			}
		}
		q.publishers[i] = nil
	}

	return nil
}
