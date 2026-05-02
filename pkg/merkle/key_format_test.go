package merkle

import "testing"

func TestKeyLayoutShouldSortByTypeThenLevelThenIndex(t *testing.T) {
	nodeLow := encodeNodeRowKey(0, 0)
	nodeHigh := encodeNodeRowKey(0, 1)
	levelHigh := encodeNodeRowKey(1, 0)
	meta := encodeMetaRowKey("leafcount")
	root := encodeRootRowKey()

	if string(nodeLow) >= string(nodeHigh) {
		t.Fatalf("expected node index ordering to be lexical")
	}
	if string(nodeHigh) >= string(levelHigh) {
		t.Fatalf("expected level ordering to be lexical")
	}
	if string(levelHigh) >= string(meta) {
		t.Fatalf("expected node rows to sort before metadata rows")
	}
	if string(meta) >= string(root) {
		t.Fatalf("expected metadata rows to sort before root rows")
	}
}
