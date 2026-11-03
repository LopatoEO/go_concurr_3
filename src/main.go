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

var (
	ErrNoBackends      = errors.New("no available backends")
	ErrBackendFailed   = errors.New("backend unavailable")
	ErrRetriesExceeded = errors.New("retry attempts exceeded")
)

type Request interface{}

type Response interface{}

type Backend interface {
	Invoke(ctx context.Context, req Request) (Response, error)
}

var _ Backend = &BackendImpl{}

// BackendImpl реализует интерфейс Backend
var _ Backend = (*BalancerImpl)(nil)

type BackendImpl struct {
	addr string
}

// Invoke выполняет запрос к конкретному экземпляру микросервиса
func (b *BackendImpl) Invoke(ctx context.Context, req Request) (Response, error) {
	timer := time.NewTimer(time.Duration(rand.Intn(maxZ-minZ+1)+minZ) * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		if rand.Intn(100) < 20 {
			return nil, ErrBackendFailed
		}
		return fmt.Sprintf("Response from %s", b.addr), nil

	}
}

// addr содержит ip:port конкретного экземпляра
func NewBackend(addr string) *BackendImpl {
	return &BackendImpl{addr: addr}
}

type BalancerImpl struct {
	backends []*BackendState
	mu       sync.Mutex
}

type BackendState struct {
	backend          Backend
	countReq         int
	unavailableUntil time.Time
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
	return &BalancerImpl{backends: backends}
}

func (b *BalancerImpl) Invoke(ctx context.Context, req Request) (Response, error) {
	const maxAttempts = 5

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		resp, err := b.invokeOnce(ctx, req)

		if err == nil {
			return resp, nil
		}

		if errors.Is(err, ErrNoBackends) {
			return nil, err
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}

	return nil, ErrRetriesExceeded
}

func (b *BalancerImpl) invokeOnce(ctx context.Context, req Request) (Response, error) {
	selectedBackend := b.getBackend()
	defer b.releaseBackend(selectedBackend)

	if selectedBackend == nil {
		return nil, ErrNoBackends
	}

	resp, err := selectedBackend.backend.Invoke(ctx, req)

	if err != nil {
		if errors.Is(err, ErrBackendFailed) {
			b.markUnavailable(selectedBackend)
		}
		return nil, err
	}
	return resp, nil
}

func (b *BalancerImpl) getBackend() *BackendState {
	b.mu.Lock()
	defer b.mu.Unlock()

	var selectedBackend *BackendState

	for _, backend := range b.backends {
		if time.Now().Before(backend.unavailableUntil) {
			continue
		}

		if selectedBackend == nil ||
			backend.countReq < selectedBackend.countReq {
			selectedBackend = backend
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

func (b *BalancerImpl) markUnavailable(backend *BackendState) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if time.Now().Before(backend.unavailableUntil) {
		return
	}

	backend.unavailableUntil = time.Now().Add(5 * time.Second)
}

type ClientPool struct{}

func (c *ClientPool) StartRequests(balancer Backend, reqNum int) {
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < reqNum; i++ {
		wg.Add(1)
		req := fmt.Sprintf("Request %d", i)
		go func() {
			defer wg.Done()
			resp, err := balancer.Invoke(ctx, req)
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
	clientPool.StartRequests(balancer, 1500)
}
