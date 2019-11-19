package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
	"github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"
	"github.com/graph-gophers/graphql-transport-ws/graphqlws"
)

const schema = `
	schema {
		query: Query
		subscription: Subscription
		mutation: Mutation
	}
	type Query {
		hi: String!
	}
	type Mutation {
		updateStatus(id: Int!, code: Int!): Status!
	}
	type Subscription {
		statusUpdated(): Status!
	}
	type Status {
		id: Int!
		code: Int!
	}
`

type resolver struct {
	rdb                      *redis.Client
	updateStatusSubscription chan *statusUpdatedSubscription
}

func newResolver(rdb *redis.Client) *resolver {
	r := &resolver{
		rdb:                      rdb,
		updateStatusSubscription: make(chan *statusUpdatedSubscription),
	}

	go r.broadcastStatusUpdates()

	return r
}

func (*resolver) Hi() string {
	return "hi"
}

type status struct {
	id, code int32
}

func (s *status) ID() int32 {
	return s.id
}

func (s *status) Code() int32 {
	return s.code
}

func (r *resolver) UpdateStatus(args struct{ ID, Code int32 }) (*status, error) {
	if err := r.rdb.Publish("status:"+strconv.Itoa(int(args.ID)), args.Code).Err(); err != nil {
		return nil, fmt.Errorf("could not publish status update: %v", err)
	}
	return &status{id: args.ID, code: args.Code}, nil
}

type statusUpdatedSubscription struct {
	id     string
	events chan<- *status
	done   <-chan struct{}
}

func (r *resolver) StatusUpdated(ctx context.Context) <-chan *status {
	ch := make(chan *status)
	r.updateStatusSubscription <- &statusUpdatedSubscription{
		id:     uuid.New().String(),
		events: ch,
		done:   ctx.Done(),
	}
	return ch
}

func (r *resolver) broadcastStatusUpdates() {
	pubsub := r.rdb.PSubscribe("status:*")

	// wait for confirmation that subscription is created before publishing anything
	if _, err := pubsub.Receive(); err != nil {
		log.Fatalln(err)
	}

	ch := pubsub.Channel()

	subscriptions := map[string]*statusUpdatedSubscription{}
	unsubscribe := make(chan string)

	for {
		select {
		case sub := <-r.updateStatusSubscription:
			log.Println("add sub: ", sub.id)
			subscriptions[sub.id] = sub
		case id := <-unsubscribe:
			delete(subscriptions, id)
			log.Println("del sub: ", id)
		case msg := <-ch:
			id, err := strconv.Atoi(strings.TrimPrefix(msg.Channel, "status:"))
			if err != nil {
				log.Println(err)
				continue
			}

			code, err := strconv.Atoi(msg.Payload)
			if err != nil {
				log.Println(err)
				continue
			}

			event := &status{
				id:   int32(id),
				code: int32(code),
			}

			fmt.Printf("event: %+v\n", event)

			for _, sub := range subscriptions {
				go func(sub *statusUpdatedSubscription) {
					select {
					case <-sub.done:
						unsubscribe <- sub.id
						return
					case sub.events <- event:
					}
				}(sub)
			}
		}
	}
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	http.HandleFunc("/", http.HandlerFunc(graphiql))

	s, err := graphql.ParseSchema(schema, newResolver(rdb))
	if err != nil {
		log.Fatalf("could not parase schema: %v", err)
	}

	graphQLHandler := graphqlws.NewHandlerFunc(s, &relay.Handler{Schema: s})
	http.HandleFunc("/graphql", graphQLHandler)

	// start HTTP server
	if err := http.ListenAndServe(":3000", nil); err != nil {
		panic(err)
	}
}
