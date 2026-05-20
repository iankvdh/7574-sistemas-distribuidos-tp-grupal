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
)

// Action is what a topology returns to the runtime after each EOF-related event.
type Action struct {
	Kind  ActionKind
	Token *Token
	EOFs  []EOFEmit
}

// EOFEmit is one InternalEOF message the runtime must publish to a specific output.
type EOFEmit struct {
	OutputIndex int
	Total       uint32
}
