package eof

// ActionKind enumerates the high-level operations the runtime must perform on
// behalf of an EOF coordination layer (ring, joiner-accumulate, etc).
type ActionKind int

const (
	// ActionNone means the runtime must do nothing in response to this event.
	ActionNone ActionKind = iota
	// ActionForwardToken means the runtime must publish Token to the next replica
	// in the ring (RING_QUEUE_OUT).
	ActionForwardToken
	// ActionEmitEOFs means the runtime must publish one InternalEOF to each entry
	// in EOFs and clear any cached upstream EOF state for the client.
	ActionEmitEOFs
	// ActionReenqueueUpstreamEOF means the runtime must re-publish the upstream
	// EOF envelope it received for this client to the input queue, so another
	// replica picks it up and restarts the ring.
	ActionReenqueueUpstreamEOF
	// ActionEmitEOFsAndForwardToken means the runtime must: (1) flush pending
	// data for the client, (2) publish one InternalEOF per entry in EOFs, and
	// (3) forward Token to the next ring replica. Used by the broadcastEOFOnClose
	// ring mode so that every replica emits EOFs at the closing pass.
	ActionEmitEOFsAndForwardToken
)

// Action is what a topology returns to the runtime after each EOF-related event.
type Action struct {
	Kind  ActionKind
	Token *Token
	EOFs  []EOFEmit
}

// EOFEmit is one InternalEOF message the runtime must publish to a specific output.
// RoutingKey is used only when the target output is a direct_exchange; otherwise
// the runtime falls back to its default sharding/key resolution.
type EOFEmit struct {
	OutputIndex int
	Total       uint32
	RoutingKey  string
}
