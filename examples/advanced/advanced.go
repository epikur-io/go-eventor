package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/epikur-io/go-eventor"
	"github.com/rs/zerolog"
)

func main() {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	observer := eventor.NewObserver(
		// default execution order is descending, highest prio first,
		// use `eventor.ExecAscending` for reverse order.
		eventor.WithExecutionOrder(eventor.ExecDescending),
		eventor.WithPanicRecover(func(ctx *eventor.EventContext, panicValue any) {
			log.Printf("recovered from panic caused by: %s (ID: %s), panic value: %v\n", ctx.EventName(), ctx.HandlerID(), panicValue)
		}),
		eventor.WithLogger(&logger),
		eventor.WithBeforeCallback(func(ctx *eventor.EventContext) error {
			// This callback runs before any event handler chain gets executed.
			// This could be used to inject context information based on the given event `ctx.EventName()`
			// or other parameters.
			ctx.Data.Set("Hello", "World")
			return nil
		}),
		eventor.WithAfterCallback(func(ctx *eventor.EventContext) {
			// This callback runs after the event handler chain was executed, even when a panic was recovered.
			// This can be used to cleanup things to trigger further program logic.
			if v := ctx.Data.Get("datetime"); v != nil {
				fmt.Println("(AfterCallback) Datetime:", v.(time.Time).String())
			}
		}),
	)

	// Add multiple event handlers / hooks
	// use "observer.Add(eventor.Handler{..})" to add a  single event handle
	err := observer.AddAll([]eventor.Handler{
		// Multiple handlers for the same event, handlers will be executed by their given order
		{
			Name: "event_a",
			ID:   "first_handler", // Unique identifier for this event handler (useful for logging & debugging)
			Prio: 30,              // Priority for event handler execution (highest first)
			Func: func(ctx *eventor.EventContext) {
				log.Printf("executing event handler: %s (ID: %s)\n", ctx.EventName(), ctx.HandlerID())

				// add some custom data to the context
				ctx.Data["datetime"] = time.Now()
			},
		},
		{
			Name: "event_a",
			ID:   "second_handler",
			Prio: 20,
			Func: func(ctx *eventor.EventContext) {
				log.Printf("executing event handler: %s (ID: %s)\n", ctx.EventName(), ctx.HandlerID())

				// do some additional work on the data provided by the previous handler
				datetime, ok := ctx.Data["datetime"].(time.Time)
				if ok {
					ctx.Data["datetime"] = datetime.Add(time.Hour * 24)
				}

				// This will stop further execution of following event handlers
				ctx.StopPropagation = true
			},
		},
		{
			Name: "event_a",
			ID:   "third_handler",
			Prio: 10,
			Func: func(ctx *eventor.EventContext) {
				// this should not be executed because `ctx.StopPropagation` was set to true.
				log.Printf("executing event handler: %s (ID: %s)\n", ctx.EventName(), ctx.HandlerID())
			},
		},
	})
	if err != nil {
		log.Fatalln(err)
	}

	// context with 1 second timeout
	expiry, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	ectx := eventor.NewEventContext(expiry)

	// Add some data to the event context
	ectx.Data.Set("foo", "bar")

	// Trigger an event
	cnt, err := observer.Dispatch("event_a", ectx) // calling `Dispatch(...)` sequentially executes all event handlers for `event_a` ordered descending by the event handlers' priority.
	if err != nil {
		log.Panic(err)
	}
	log.Printf("Executed %d handlers\n", cnt)
	log.Printf("Datetime:  %v\n", ectx.Data["datetime"])
}
