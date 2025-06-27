package azure

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

type azureEnumerator struct {
	ctx    context.Context
	pager  *runtime.Pager[aztables.ListEntitiesResponse]
	limit  int
	total  int
	buffer []*kv.Item
	index  int
	err    error
}

func AzureEnumerator(ctx context.Context, pager *runtime.Pager[aztables.ListEntitiesResponse], limit int) enumerators.Enumerator[*kv.Item] {
	return &azureEnumerator{
		ctx:   ctx,
		pager: pager,
		limit: limit,
	}
}

func (e *azureEnumerator) MoveNext() bool {
	if e.limit > 0 && e.total >= e.limit {
		return false
	}
	if e.index < len(e.buffer) {
		return true
	}
	for e.pager.More() {
		resp, err := e.pager.NextPage(e.ctx)
		if err != nil {
			e.err = fmt.Errorf("failed to fetch page: %w", err)
			return false
		}
		e.buffer = e.buffer[:0]
		for _, raw := range resp.Entities {
			var entity Entity
			if err := json.Unmarshal(raw, &entity); err != nil {
				e.err = fmt.Errorf("failed to decode entity: %w", err)
				return false
			}
			e.buffer = append(e.buffer, &kv.Item{
				PK: lexkey.PrimaryKey{
					PartitionKey: entity.PartitionKey,
					RowKey:       entity.RowKey,
				},
				Value: entity.Value,
			})
		}
		e.index = 0
		if len(e.buffer) > 0 {
			return true
		}
	}
	return false
}

func (e *azureEnumerator) Current() (*kv.Item, error) {
	if e.index >= len(e.buffer) {
		return nil, e.err
	}
	item := e.buffer[e.index]
	e.index++
	e.total++
	return item, nil
}

func (e *azureEnumerator) Dispose() {}

func (e *azureEnumerator) Err() error { return e.err }
