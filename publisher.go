package relay

import (
	"bytes"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"strconv"
	"sync"
	"time"
)

// Publisher is a type that is used only for publishing messages to a single queue.
// Multiple Publishers can multiplex a single relay.
type Publisher struct {
	conf        *Config
	queue       string
	channel     *amqp.Channel
	contentType string
	mode        uint8
	buf         bytes.Buffer
	errCh       chan *amqp.Error
	msgCount    uint64
	finish      chan struct{}
	confirms    sync.WaitGroup
	m           sync.Mutex
}

// Publish will send the message to the server to be consumed
func (p *Publisher) Publish(in interface{}) error {

	// Encode the message
	conf := p.conf
	buf := &p.buf
	buf.Reset()
	if err := conf.Serializer.RelayEncode(buf, in); err != nil {
		return fmt.Errorf("Failed to encode message! Got: %s", err)
	}

	// Lock so multiple go routines can't publish at the same time
	// or publish to a closed channel. Encoding happens first so
	// that the lock isn't held during that time.
	p.m.Lock()

	// Check for close
	if p.channel == nil {
		p.m.Unlock()
		return ChannelClosed
	}

	if !p.conf.DisablePublishConfirm && p.finish == nil {
		p.finish = make(chan struct{}, 0)
	}

	// Format the message
	msg := amqp.Publishing{
		DeliveryMode: p.mode,
		Timestamp:    time.Now().UTC(),
		ContentType:  p.contentType,
		Body:         buf.Bytes(),
	}

	// Check for a message ttl
	if p.conf.MessageTTL > 0 {
		msec := int(p.conf.MessageTTL / time.Millisecond)
		msg.Expiration = strconv.Itoa(msec)
	}

	var ack, nack chan uint64
	if !p.conf.DisablePublishConfirm {
		// Add channels to notify of ack or nack
		ack = make(chan uint64, 1)
		nack = make(chan uint64, 1)
		p.channel.NotifyConfirm(ack, nack)
	}

	// Publish the message and increment the counter within the lock to prevent
	// race condition on msgNumber
	if err := p.channel.Publish(conf.Exchange, p.queue, false, false, msg); err != nil {
		p.m.Unlock()
		return fmt.Errorf("Failed to publish to '%s'! Got: %s", p.queue, err)
	}

	p.msgCount++
	msgNumber := p.msgCount

	if !p.conf.DisablePublishConfirm {
		p.confirms.Add(1)
	}

	// Unlock as soon as the publish has occured so that other go routines can
	// publish while the current one blocks waiting for a confirm.
	p.m.Unlock()

	// Check if we wait for confirmation
	if !p.conf.DisablePublishConfirm {
		defer p.confirms.Done()
		for {
			// Because multiple go routines can be waiting for an ack or nack
			// simultaneously, each must block until the correct message number
			// arrives or an error occurs before continue.
			select {
			case seq, ok := <-ack:
				if !ok {
					return ChannelClosed
				} else if seq == msgNumber {
					return nil
				}
			case seq, ok := <-nack:
				if !ok {
					return ChannelClosed
				} else if seq == msgNumber {
					return fmt.Errorf("Failed to publish to '%s'! Got negative ack.", p.queue)
				}
			case err, ok := <-p.errCh:
				if !ok {
					return ChannelClosed
				}
				log.Printf("[ERR] Publisher got error: (Code %d Server: %v Recoverable: %v) %s",
					err.Code, err.Server, err.Recover, err.Reason)
				return fmt.Errorf("Failed to publish to '%s'! Got: %s", err.Error())
			case <-p.finish:
				return TimedOut
			}
		}
	}

	return nil
}

// WaitForConfirms blocks until all confirms have arrived or the optional
// timeout occurs. If the timeout occurs before all confirms have arrived the
// goroutines blocked on Publish will return immediately with a TimedOut error.
//
// Returns true if all confirms arrived before timeout period. This value is
// undefined if WaitForConfirms is called from multiple goroutines and Publish
// is called concurrently.
//
// WaitForConfirms is useful for implementing a graceful exit.
func (p *Publisher) WaitForConfirms(timeout *time.Duration) bool {
	p.m.Lock()
	defer p.m.Unlock()

	if p.finish == nil {
		return false
	}

	complete := make(chan struct{}, 0)
	if timeout != nil {
		go func() {
			select {
			case <-time.Tick(*timeout):
				close(p.finish) // Stop all goroutines waiting for ack or nack.

				// Reset finish channel so it can be recreated if Publish is
				// called again. Furthermore if WaitForConfirms is called
				// multiple times and a timeout occurs all occurances should
				// return the correct boolean.
				p.finish = nil
			case <-complete:
				// All ack or nacks completed so exit this goroutine without closing
				// finish channel.
			}
		}()
	}
	p.confirms.Wait()
	close(complete)
	return p.finish != nil
}

// Close will shutdown the publisher
func (p *Publisher) Close() error {
	p.m.Lock()
	defer p.m.Unlock()

	// Make sure close is idempotent
	if p.channel == nil {
		return nil
	}
	defer func() {
		p.channel = nil
	}()
	return p.channel.Close()
}
