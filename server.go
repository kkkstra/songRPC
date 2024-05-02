package songRPC

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kkkstra/songRPC/codec"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
)

const MagicNumber = 0x131cad6

type Option struct {
	MagicNumber int        // 用于标识 songRPC 请求
	CodecType   codec.Type // 编解码方式
}

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   codec.GobType,
}

type Server struct{}

type request struct {
	header       *codec.Header
	argv, replyv reflect.Value
}

func NewServer() *Server {
	return &Server{}
}

var (
	DefaultServer  = NewServer()
	invalidRequest = struct{}{}
)

// Accept 等待连接并处理请求
func (server *Server) Accept(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("rpc server: accept error: ", err)
			return
		}
		go server.ServeConn(conn)
	}
}

func Accept(listener net.Listener) { DefaultServer.Accept(listener) }

func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer conn.Close()

	var option Option
	if err := json.NewDecoder(conn).Decode(&option); err != nil {
		log.Println("rpc server: decode option error: ", err)
		return
	}
	if option.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x\n", option.MagicNumber)
		return
	}
	f := codec.NewCodecFuncMap[option.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid code-type %s\n", option.CodecType)
		return
	}

	server.serveCodec(f(conn))
}

func (server *Server) serveCodec(cc codec.Codec) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)

	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				break // 无法 recover
			}
			req.header.Error = err.Error()
			server.sendResponse(cc, req.header, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg)
	}
	wg.Wait()
	_ = cc.Close()
}

func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var header codec.Header

	if err := cc.ReadHeader(&header); err != nil {
		if err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}

	return &header, nil
}

func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	header, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}

	req := &request{header: header}
	// TODO: now we don't know the type of request argv
	// day 1, just suppose it's string
	req.argv = reflect.New(reflect.TypeOf(""))
	if err = cc.ReadBody(req.argv.Interface()); err != nil {
		log.Println("rpc server: read argv err:", err)
	}
	return req, nil
}

func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println(req.header, req.argv.Elem())
	req.replyv = reflect.ValueOf(fmt.Sprintf("songRPC resp %d", req.header.Seq))
	server.sendResponse(cc, req.header, req.replyv.Interface(), sending)
}

func (server *Server) sendResponse(cc codec.Codec, header *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()

	if err := cc.Write(header, body); err != nil {
		log.Println("rpc server: write header err:", err)
	}
}
