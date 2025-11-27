package topostream

//go:generate protoc --proto_path=../../spec/ --go_out=. topo_stream.proto
//go:generate protoc --proto_path=../../spec/ --go_out=. topo_stream_message_key.proto
