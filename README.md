# GCache

[![GoDoc](https://godoc.org/github.com/limpo1989/gcache?status.svg)](https://pkg.go.dev/github.com/limpo1989/gcache?tab=doc)

Cache library for golang. It supports expirable Cache, LFU, LRU and ARC.

## Features

* Supports expirable Cache, LFU, LRU and ARC.
* Auto-renewal cache items. (Optional)
* Goroutine safe.
* Supports event handlers which evict, purge, and add entry. (Optional)
* Automatically load cache if it doesn't exists. (Optional)
* Supports context.
* Supports shared cache.
* Supports generic interface.
* Supports managed by the Arena allocator. (Optional)

## Install

```
$ go get github.com/limpo1989/gcache
```

## Example

```go
package main

import (
	"context"
	"fmt"

	"github.com/limpo1989/gcache"
)

func main() {
	gc := gcache.New[string, string](20).
		LRU().
		LoaderFunc(func(ctx context.Context, key string) (*string, error) {
			value := "ok"
			return &value, nil
		}).
		Build()
	value, err := gc.Get(context.Background(), "key")
	if err != nil {
		panic(err)
	}
	fmt.Println("Get:", *value)
}
```

## Acknowledgement
This project initial code based from [gcache](https://github.com/bluele/gcache) written by [bluele](https://github.com/bluele)