package storage

// SelfMaintainingStorage is storage that recovers expired leases and applies
// retention itself, such as a storage service that runs them for every
// queue. When MaintainsItself reports true the engine runs neither, and
// rejects its own retention settings rather than silently ignoring them:
// configure retention on the storage.
type SelfMaintainingStorage interface {
	MaintainsItself() bool
}
