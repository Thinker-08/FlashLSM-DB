// Package record implements the record framing shared by the WAL and MANIFEST:
// crc32c (4 bytes LE, over length and payload) | length (4 bytes LE) | payload.
package record
