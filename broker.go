// Created by:	M Liu, Aug 15, 2022
// Cache broker implementation
package cache

import (
	"errors"
	"sync"
	"time"
)

// Core functionality of the cache broker
type CacheBroker interface {
	publish(subscriber string, message interface{}) error
	subscribe(subscriber string) (<-chan interface{}, error)
	unsubscribe(subscriber string, sub <-chan interface{}) error
	broadcast(message interface{}, subscriber chan interface{})
	setConditions(capacity int)
	close()
}

// The implementation of the cache broker
type BrokerImpl struct {
	exit        chan bool
	capacity    int // capacity of the channel
	concurrency int // number of threads to process the messages
	subscribers map[string]chan interface{}
	sync.RWMutex
}

// Publish one message
func (b *BrokerImpl) publish(subscriber string, message interface{}) error {
	select {
	case <-b.exit:
		return errors.New("broker closed")
	default:
	}

	b.RLock()
	ch, ok := b.subscribers[subscriber]
	b.RUnlock()

	if !ok {
		return nil
	}

	ch <- message
	return nil
}

// Broadcast the message
func (b *BrokerImpl) broadcast(message interface{}) error {
	pub := func(start int) {
		counter := 0
		for _, ch := range b.subscribers {
			if counter%start == 0 {
				select {
				case ch <- message:
				case <-time.After(time.Millisecond * 5):
				case <-b.exit:
					return
				}
			}
			counter++
		}
	}
	for threads := 0; threads < b.concurrency; threads++ {
		go pub(threads)
	}
	return nil
}

// Subscribe to the cache
func (b *BrokerImpl) subscribe(subscriber string) (<-chan interface{}, error) {
	select {
	case <-b.exit:
		return nil, errors.New("broker closed")
	default:
	}

	b.RLock()
	ch, ok := b.subscribers[subscriber]
	b.RUnlock()

	if !ok {
		ch = make(chan interface{}, b.capacity)
		b.Lock()
		b.subscribers[subscriber] = ch
		b.Unlock()
	}

	return ch, nil
}

// Remove the subscription
func (b *BrokerImpl) unsubscribe(subscriber string) error {
	select {
	case <-b.exit:
		return errors.New("broker closed")
	default:
	}

	b.RLock()
	ch, ok := b.subscribers[subscriber]
	b.RUnlock()

	if !ok {
		return nil
	}

	close(ch)
	delete(b.subscribers, subscriber)

	return nil
}

// Close the broker
func (b *BrokerImpl) close() {
	select {
	case <-b.exit:
		return
	default:
		close(b.exit)
		b.Lock()
		b.subscribers = make(map[string]chan interface{})
		b.Unlock()
	}
	return
}
