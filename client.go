// Created by:	M Liu, Aug 15, 2022

package github.com/marvinliu1/channel-cache

// Client
type Client struct {
	bro *BrokerImpl
}

// Constructor
func NewClient(cap int, concurr int) *Client {
	return &Client{
		bro: NewBroker(cap, concurr),
	}
}

// Create a broker instance
func NewBroker(cap int, concurr int) *BrokerImpl {
	brokerImpl := BrokerImpl{subscribers: make(map[string]chan interface{}), capacity: cap, concurrency: concurr}
	return &brokerImpl
}

// Publish the message
func (c *Client) Publish(subscriber string, message interface{}) error {
	return c.bro.publish(subscriber, message)
}

// Broadcast the message
func (c *Client) Broadcast(message interface{}) error {
	return c.bro.broadcast(message)
}

// Subscribe to the cache
func (c *Client) Subscribe(subscriber string) (<-chan interface{}, error) {
	return c.bro.subscribe(subscriber)
}

// Unsubscriber from the cache
func (c *Client) Unsubscribe(subscriber string) error {
	return c.bro.unsubscribe(subscriber)
}

// Close the client
func (c *Client) Close() {
	c.bro.close()
}

// Read the message
func (c *Client) GetPayLoad(sub <-chan interface{}) interface{} {
	for val := range sub {
		if val != nil {
			return val
		}
	}
	return nil
}
