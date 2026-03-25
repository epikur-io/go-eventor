# A thread-safe event manager for Go

`go-eventor` provides a **robust, thread-safe implementation of the Observer pattern** for Go.
It lets you define events, attach multiple prioritized handlers, and trigger them safely and efficiently across goroutines.

## Features

- **Thread-safe** event registration and triggering
  - The observer uses internal synchronization (mutexes) to ensure:
  - Safe concurrent registration/removal of handlers
  - Correct ordering and consistent event triggering
  - No race conditions when modifying shared context data
- **Priority-based** handler execution (ascending or descending)
- **Before/After callbacks** for custom logic injection
- **Context propagation** and safe data sharing
- **StopPropagation** support to halt handler chains
- **Flexible logging** integration (via zerolog)
- **Lightweight, composable API**

## Core Concepts

### Observer

The Observer is the main event manager. It maintains:

- A registry of event handlers grouped by event name
- Optional before/after hooks
- Dispaching of events and execute event handlers
- Thread-safe synchronization

### Event Handler

An event handler defines how to handle a specific event.
It contains:

```go
type Handler struct {
    EventName string
    ID        string
    Prio      int
    Func      func(ctx *EventContext)
}
```

Handlers for the same event are executed based on their priority.
Use ExecDescending (default) or ExecAscending to control the order.

> **Note:** The uniqueness of the `EventHandler.ID` is not enforced. Its primary use is to help debugging and understand the event logs better.

### Event Context

Event context carries:

```go
type EventContext struct {
	Context         context.Context
	StopPropagation bool
	Data            Data
}
func (ctx *EventContext) EventName() string
func (ctx *EventContext) HandlerID() string
func (ctx *EventContext) Err() error
```

- The context.Context (`ctx.GoContext`) for cancellation/timeouts
- The EventName and HandlerID (`ctx.EventName()` & `ctx.HandlerID()`)
- A Data map for sharing information between handlers
- A StopPropagation flag to halt execution mid-chain
- Potetial errors (`ctx.Err()`)


## Quick Start Example

A basic example can be found [here](./examples/basic/basic.go).

```go
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
```

## Advanced Usage

### Adding Handlers Individually

```go
observer.Add(eventor.EventHandler{
	EventName: "user_signup",
	ID:        "send_welcome_email",
	Prio:      50,
	Func: func(ctx *eventor.EventContext) {
		fmt.Println("Sending welcome email...")
	},
})
```

### Removing Handlers

```go
observer.Remove("user_signup", "send_welcome_email")
```

### Triggering Events Concurrently

```go
// Note: each goroutine requires its own event context!
go observer.Dispatch("event_a", eventor.NewEventContext(context.Background()))
```

### Custom Execution Order

```go
observer := eventor.NewObserver(
	// Reverse execution order, lowest prio first (default: highest prio first).
	eventor.WithExecutionOrder(eventor.ExecAscending),
)
```

### Using Before and After Callbacks

```go
// When the callback returns an error, the error will be forwarded and returned
// by the `Dispatch` method, no event handlers will be executed.
eventor.WithBeforeCallback(func(ctx *eventor.EventContext) error {
	ctx.Data["timestamp"] = time.Now()
	return nil
})
```
