package event

// Sink consumes captured events. The proxy and mitmproxy addon both target
// implementations of this interface. Emit must be non-blocking and safe for
// concurrent calls — implementers handle backpressure (e.g. drop-on-full).
type Sink interface {
	Emit(Event)
}
