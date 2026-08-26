package kvstore

import (
	"bytes"
	"encoding/gob"
)

// CommandType represents the type of KV operation
type CommandType uint8

const (
	CmdSet CommandType = iota
	CmdDelete
)

// Command represents the payload stored inside a Raft LogEntry's Command bytes
type Command struct {
	Type  CommandType
	Key   string
	Value string // empty for CmdDelete

}

// Encode serializes a Command into raw bytes to store in pb.LogEntry
func (c Command) Encode() ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeCommand deserializes raw bytes back into a Command
func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	if err := decoder.Decode(&cmd); err != nil {
		return Command{}, err
	}
	return cmd, nil
}
