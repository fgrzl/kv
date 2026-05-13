package kv

import (
	"testing"

	"github.com/fgrzl/lexkey"
	"github.com/stretchr/testify/assert"
)

func TestShouldReverseItemsInPlace(t *testing.T) {
	// Arrange
	items := []*Item{
		{PK: lexkey.NewPrimaryKey(lexkey.Encode("p"), lexkey.Encode("a")), Value: []byte("1")},
		{PK: lexkey.NewPrimaryKey(lexkey.Encode("p"), lexkey.Encode("b")), Value: []byte("2")},
		{PK: lexkey.NewPrimaryKey(lexkey.Encode("p"), lexkey.Encode("c")), Value: []byte("3")},
	}
	original := make([]*Item, len(items))
	copy(original, items)

	// Act
	ReverseItems(items)

	// Assert
	assert.Len(t, items, 3)
	assert.Equal(t, original[2], items[0])
	assert.Equal(t, original[1], items[1])
	assert.Equal(t, original[0], items[2])
}

func TestShouldSortItemsByDirection(t *testing.T) {
	tests := []struct {
		name      string
		direction SortDirection
		input     []*Item
		want      [][]byte
	}{
		{
			name:      "ascending",
			direction: Ascending,
			input: []*Item{
				{PK: lexkey.NewPrimaryKey(lexkey.Encode("p"), lexkey.Encode("c")), Value: []byte("3")},
				{PK: lexkey.NewPrimaryKey(lexkey.Encode("p"), lexkey.Encode("a")), Value: []byte("1")},
				{PK: lexkey.NewPrimaryKey(lexkey.Encode("p"), lexkey.Encode("b")), Value: []byte("2")},
			},
			want: [][]byte{[]byte("1"), []byte("2"), []byte("3")},
		},
		{
			name:      "descending",
			direction: Descending,
			input: []*Item{
				{PK: lexkey.NewPrimaryKey(lexkey.Encode("p"), lexkey.Encode("a")), Value: []byte("1")},
				{PK: lexkey.NewPrimaryKey(lexkey.Encode("p"), lexkey.Encode("c")), Value: []byte("3")},
				{PK: lexkey.NewPrimaryKey(lexkey.Encode("p"), lexkey.Encode("b")), Value: []byte("2")},
			},
			want: [][]byte{[]byte("3"), []byte("2"), []byte("1")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			items := append([]*Item(nil), tt.input...)
			SortItems(items, tt.direction)
			assert.Len(t, items, 3)
			for i, w := range tt.want {
				assert.Equal(t, w, items[i].Value)
			}
		})
	}
}

func TestShouldSortItemsByPartitionKeyFirst(t *testing.T) {
	// Arrange
	items := []*Item{
		{PK: lexkey.NewPrimaryKey(lexkey.Encode("q"), lexkey.Encode("a")), Value: []byte("qa")},
		{PK: lexkey.NewPrimaryKey(lexkey.Encode("p"), lexkey.Encode("b")), Value: []byte("pb")},
		{PK: lexkey.NewPrimaryKey(lexkey.Encode("p"), lexkey.Encode("a")), Value: []byte("pa")},
	}

	// Act
	SortItems(items, Ascending)

	// Assert
	assert.Equal(t, []byte("pa"), items[0].Value)
	assert.Equal(t, []byte("pb"), items[1].Value)
	assert.Equal(t, []byte("qa"), items[2].Value)
}
