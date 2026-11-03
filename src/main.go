package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

/*
Есть приложение с микросервисной архитектурой.
Микросервис можно абстрагировать с помощью интерфейса Backend.
Для доступа к одному экземпляру микросервиса можно использовать
тип BackendImpl, который уже реализован.


Для каждого микросервиса есть несколько десятков запущенных
экземпляров, каждый из которых доступен по своему адресу addr.
Однако отдельные экземпляры микросервиса ненадежны:
они могут падать, быть недоступными либо перегруженными.
Поэтому вам нужно реализовать тип Balancer, который также реализует
интерфейс Backend и осуществляет у балансировку нагрузки
между экземплярами микросервиса, выбирая каждый раз **наименее нагруженный** экземпляр.
*/

const (
	minZ = 1
	maxZ = 8
)

type Request interface{}

type Response interface{}

type Backend interface {
	Invoke(ctx context.Context, req Request) (Response, error)
}

var _ Backend = &BackendImpl{}

type BackendImpl struct {
	addr string
}

// Invoke выполняет запрос к конкретному экземпляру микросервиса
func (b *BackendImpl) Invoke(ctx context.Context, req Request) (Response, error) {
	timer := time.NewTimer(time.Duration(rand.Intn(maxZ-minZ+1)) + minZ*time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		if rand.Intn(100) < 20 {
			return nil, errors.New("backend unavailable")
		}
		return fmt.Sprintf("Response from %s", b.addr), nil

	}
}

// BackendImpl реализует интерфейс Backend

// addr содержит ip:port конкретного экземпляра
func NewBackend(addr string) *BackendImpl {
	return &BackendImpl{addr: addr}
}

type Balancer interface {
	Balance(req Request) (Response, error)
	Close()
	getBackend() *BackendState
	releaseBackend(selectedBackend *BackendState)
}

type BackendState struct {
	backend  Backend
	countReq int
}

type BalancerImpl struct {
	backends  []*BackendState
	mu        sync.Mutex
	ctx       context.Context
	ctxCancel context.CancelFunc
}

// addrs содержат адреса всех балансируемых экземпляров
func NewBalancer(addrs []string) *BalancerImpl {
	backends := make([]*BackendState, 0, len(addrs))
	for _, addr := range addrs {
		backend := NewBackend(addr)
		bs := &BackendState{
			backend:  backend,
			countReq: 0,
		}
		backends = append(backends, bs)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &BalancerImpl{backends: backends, ctx: ctx, ctxCancel: cancel}
}

func (b *BalancerImpl) Close() {
	b.ctxCancel()
}

func (b *BalancerImpl) getBackend() *BackendState {
	b.mu.Lock()
	defer b.mu.Unlock()

	var min int = -1
	var selectedBackend *BackendState
	for i, backend := range b.backends {
		if min == -1 || backend.countReq < min {
			min = backend.countReq
			selectedBackend = b.backends[i]
		}
	}

	if selectedBackend != nil {
		selectedBackend.countReq++
	}

	return selectedBackend
}

func (b *BalancerImpl) releaseBackend(selectedBackend *BackendState) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if selectedBackend != nil {
		selectedBackend.countReq--
	}
}

func (b *BalancerImpl) Balance(req Request) (Response, error) {
	select {
	case <-b.ctx.Done():
		return nil, errors.New("balancer closed")
	default:
	}
	selectedBackend := b.getBackend()
	if selectedBackend == nil {
		return nil, errors.New("no available backends")
	}
	defer b.releaseBackend(selectedBackend)
	return selectedBackend.backend.Invoke(b.ctx, req)
}

type ClientPool struct{}

func (c *ClientPool) StartRequests(balancer Balancer, reqNum int) {
	var wg sync.WaitGroup

	for i := 0; i < reqNum; i++ {
		wg.Add(1)
		req := fmt.Sprintf("Request %d", i)
		go func() {
			defer wg.Done()
			resp, err := balancer.Balance(req)
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println(resp)
		}()
	}
	wg.Wait()
}

func main() {
	addrs := []string{
		"localhost:8080",
		"localhost:8081",
		"localhost:8082",
		"localhost:8083",
	}
	balancer := NewBalancer(addrs)

	clientPool := &ClientPool{}
	clientPool.StartRequests(balancer, 10)
}
