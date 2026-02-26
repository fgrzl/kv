package azure

import (
	"context"
	"fmt"

	client "github.com/fgrzl/azkit/tables"
	"github.com/fgrzl/enumerators"
	kv "github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

type azureEnumerator struct {
	ctx    context.Context
	pager  *client.ListEntitiesPager
	limit  int
	total  int
	buffer []*kv.Item
	index  int
	err    error
}

// AzureEnumerator returns an enumerator that pages through list-entity results
// and maps each entity to a kv.Item. limit caps the total number of items (0 = no cap).
func AzureEnumerator(ctx context.Context, pager *client.ListEntitiesPager, limit int) enumerators.Enumerator[*kv.Item] {
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
	for !e.pager.IsDone() {
		entities, err := e.pager.FetchPage(e.ctx)
		if err != nil {
			e.err = fmt.Errorf("failed to fetch page: %w", err)
			return false
		}
		e.buffer = e.buffer[:0]
		for _, ent := range entities {
			var pk, rk lexkey.LexKey
			if err := pk.FromHexString(ent.PartitionKey); err != nil {
				e.err = fmt.Errorf("failed to decode partition key: %w", err)
				return false
			}
			if err := rk.FromHexString(ent.RowKey); err != nil {
				e.err = fmt.Errorf("failed to decode row key: %w", err)
				return false
			}
			e.buffer = append(e.buffer, &kv.Item{
				PK: lexkey.PrimaryKey{
					PartitionKey: pk,
					RowKey:       rk,
				},
				Value: ent.Value,
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
