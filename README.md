# A basic kv store

```go
func main() {
	store, err := pebble.NewPebbleStore("./mydb")
	defer store.Close()
	if err != nil {
		log.Printf("Failed to initialize Pebble DB: %v\n", err)
		return
	}
	// Put an item
	item := &kv.Item{Key: kv.EncodedKey("key1"), Value: []byte("value1")}
	err = store.Put(item)

	// Get an item
	result, err := store.Get(item.Key)
	if err != nil {
		log.Printf("Failed to get item: %v\n", err)
		return
	}
	log.Printf("%s\n", string(result.Value))

	// Put multiple items
	items := []*kv.Item{
		{Key: kv.EncodedKey("key1"), Value: []byte("value1")},
		{Key: kv.EncodedKey("key2"), Value: []byte("value2")},
		{Key: kv.EncodedKey("key3"), Value: []byte("value3")},
		{Key: kv.EncodedKey("key4"), Value: []byte("value4")},
		{Key: kv.EncodedKey("key5"), Value: []byte("value6")},
	}
	err = store.PutBatch(items)
	if err != nil {
		log.Printf("Failed to put batch: %v\n", err)
		return
	}

	// Query items
	query := &kv.QueryArgs{
		StartKey: kv.EncodedKey("key2"),
		EndKey:   kv.EncodedKey("key4"),
		Operator: kv.Between,
	}

	// Enumerate the results
	enumerator := store.Enumerate(query)
	defer enumerator.Dispose()
	for enumerator.MoveNext() {
		item, err := enumerator.Current()
		if err != nil {
			log.Printf("Failed to retrieve item: %v\n", err)
			continue
		}
		log.Printf("%s\n", string(item.Value))
	}

	// Or use the Query method to collect the results as a slice
	results, err := store.Query(query, kv.Ascending)
	if err != nil {
		log.Printf("Failed to query items: %v\n", err)
		return
	}
	log.Printf("Results: %d\n", len(results))
}
```
