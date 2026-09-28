// Package lsmkv is an embeddable, crash-safe, ordered key-value store built on a
// log-structured merge tree. A DB is safe for concurrent use; a Batch or an Iterator
// belongs to one goroutine at a time, and a Snapshot may be shared.
package lsmkv
