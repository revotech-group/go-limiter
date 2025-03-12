package httplimit

import (
	"context"
	"log"
	"net/http"
	"sort"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/revotech-group/go-limiter"
	"github.com/revotech-group/go-limiter/memorystore"
	"github.com/revotech-group/go-redisstore"
)

type QuotaLimiter struct {
	stores       []limiter.Store
	redisConnStr *string
}

type Quota struct {
	Name     string
	Tokens   uint64
	Interval time.Duration
}

type QuotaOption func(*QuotaLimiter)

func WithRedisConnStr(connStr string) QuotaOption {
	return func(ql *QuotaLimiter) {
		ql.redisConnStr = &connStr
	}
}

func NewQuotaLimiter(options ...QuotaOption) *QuotaLimiter {
	ql := &QuotaLimiter{}
	for _, opt := range options {
		opt(ql)
	}
	return ql
}

type QuotaMiddleware func(http.Handler) http.Handler

func (ql *QuotaLimiter) createStore(quota Quota) limiter.Store {
	var store limiter.Store
	var err error

	if ql.redisConnStr != nil {
		store, err = redisstore.New(&redisstore.Config{
			Tokens:   quota.Tokens,
			Interval: quota.Interval,
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", *ql.redisConnStr)
			},
		})
		if err != nil {
			log.Fatalf("Error creating redis store for quota '%s': %v", quota.Name, err)
		}
	} else {
		store, err = memorystore.New(&memorystore.Config{
			Tokens:   quota.Tokens,
			Interval: quota.Interval,
		})
		if err != nil {
			log.Fatalf("Error creating memory store for quota '%s': %v", quota.Name, err)
		}
	}

	ql.stores = append(ql.stores, store)
	return store
}

func (ql *QuotaLimiter) NewMiddleware(quotas []Quota, keyFunc KeyFunc) QuotaMiddleware {
	return func(next http.Handler) http.Handler {

		type quotaMiddlewarePair struct {
			middleware *Middleware
			interval   time.Duration
		}

		var pairs []quotaMiddlewarePair

		for _, quota := range quotas {
			keyWrapper := func(qName string) KeyFunc {
				return func(r *http.Request) (string, error) {
					k, err := keyFunc(r)
					if err != nil {
						return "", err
					}
					return qName + k, nil
				}
			}(quota.Name)

			store := ql.createStore(quota)
			mw, err := NewMiddleware(store, keyWrapper)
			if err != nil {
				log.Fatalf("Failed to create HTTP limiter middleware for quota '%s': %v", quota.Name, err)
			}

			pairs = append(pairs, quotaMiddlewarePair{
				middleware: mw,
				interval:   quota.Interval,
			})
		}

		sort.Slice(pairs, func(i, j int) bool {
			return pairs[i].interval > pairs[j].interval
		})

		finalHandler := next
		for _, pair := range pairs {
			finalHandler = pair.middleware.Handle(finalHandler)
		}

		return finalHandler
	}
}

func (ql *QuotaLimiter) Cleanup(ctx context.Context) {
	for _, store := range ql.stores {
		store.Close(ctx)
	}
}
