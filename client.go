package songRPC

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kkkstra/songRPC/codec"
	"io"
	"log"
	"net"
	"sync"
)

type Call struct {
	Seq           uint64
	ServiceMethod string // 格式 <service>.<method>
	Args          interface{}
	Reply         interface{}
	Error         error
	Done          chan *Call
}

func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	cc       codec.Codec
	option   *Option
	sending  sync.Mutex
	header   codec.Header     // 请求发送是互斥的，因此每个客户端只需要一个 header
	mu       sync.Mutex       // 保护下面的字段
	seq      uint64           // 请求编号
	pending  map[uint64]*Call // 未发送的请求
	closing  bool             // 用户主动关闭
	shutdown bool             // 服务端关闭
}

var (
	_           io.Closer = (*Client)(nil)
	ErrShutdown           = errors.New("connection is shut down")
)

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return ErrShutdown
	}
	c.closing = true
	return nil
}

func (c *Client) IsAvailable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closing && !c.shutdown
}

func (c *Client) registerCall(call *Call) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.shutdown || c.closing {
		return 0, ErrShutdown
	}

	call.Seq = c.seq
	c.pending[call.Seq] = call
	c.seq++
	return call.Seq, nil
}

func (c *Client) removeCall(seq uint64) *Call {
	c.mu.Lock()
	defer c.mu.Unlock()

	call := c.pending[seq]
	delete(c.pending, seq)
	return call
}

func (c *Client) terminateCalls(err error) {
	c.sending.Lock()
	defer c.sending.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shutdown = true
	for _, call := range c.pending {
		call.Error = err
		call.done()
	}
}

func (c *Client) receive() {
	var err error
	for err == nil {
		var header codec.Header
		if err = c.cc.ReadHeader(&header); err != nil {
			break
		}
		call := c.removeCall(header.Seq)
		switch {
		// 请求未发送完整，或者因为其他原因被取消，但是服务端仍旧处理了
		case call == nil:
			err = c.cc.ReadBody(nil)
		// 服务端处理出错
		case header.Error != "":
			call.Error = errors.New(header.Error)
			err = c.cc.ReadBody(nil)
			call.done()
		default:
			err = c.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	c.terminateCalls(err)
}

func NewClient(conn net.Conn, option *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[option.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", option.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}

	// 将 option 发送给服务端
	if err := json.NewEncoder(conn).Encode(option); err != nil {
		log.Println("rpc client: option error:", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), option), nil
}

func newClientCodec(cc codec.Codec, option *Option) *Client {
	client := &Client{
		seq:     1, // seq starts with 1, 0 means invalid call
		cc:      cc,
		option:  option,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

func parseOptions(options ...*Option) (*Option, error) {
	if len(options) == 0 {
		return DefaultOption, nil
	}
	if len(options) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	if options[0] == nil {
		return DefaultOption, nil
	}
	option := options[0]
	option.MagicNumber = DefaultOption.MagicNumber
	if option.CodecType == "" {
		option.CodecType = DefaultOption.CodecType
	}
	return option, nil
}

// Dial 通过网络地址连接到 RPC 服务端
func Dial(network, address string, options ...*Option) (client *Client, err error) {
	option, err := parseOptions(options...)
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	return NewClient(conn, option)
}

func (c *Client) send(call *Call) {
	c.sending.Lock()
	defer c.sending.Unlock()

	seq, err := c.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	c.header.ServiceMethod = call.ServiceMethod
	c.header.Seq = seq
	c.header.Error = ""

	// 编码并发送请求
	if err := c.cc.Write(&c.header, call.Args); err != nil {
		call := c.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go 异步调用
func (c *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("done channel is unbuffered")
	}

	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	c.send(call)
	return call
}

// Call 同步调用
func (c *Client) Call(serviceMethod string, args, reply interface{}) error {
	call := <-c.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}
