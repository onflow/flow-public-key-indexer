package main

import (
	"testing"

	"github.com/onflow/flow-go-sdk/client"
)

func TestChunkEventRangeQueryEven(t *testing.T) {
	events := ChunkEventRangeQuery(250, 0, uint64(250), "event_name")
	assertBlockRange(t, events[0], 250)
	if len(events) != 1 {
		t.Errorf("Should have been one chunk: %d", len(events))
	}
}
func TestMegaChunks(t *testing.T) {
	chuk := 30
	blockRange := uint64(250)
	start := uint64(67493041)
	end := start + uint64(int(blockRange)*chuk)
	startResults := map[int]uint64{}
	endResults := map[int]uint64{}

	holderValue := start
	for i := 1; i <= chuk; i++ {
		startResults[i] = holderValue
		tmp := holderValue + blockRange
		endResults[i] = tmp
		holderValue = tmp + 1
		if endResults[i] > end {
			endResults[i] = end
		}
	}

	events := ChunkEventRangeQuery(int(blockRange), start, end, "event_name")

	for i := 1; i < len(events); i++ {
		ev := events[i-1]
		startBlk := startResults[i]
		endBlk := endResults[i]
		assertBlockRange(t, ev, blockRange)
		assertBlockValues(t, ev, startBlk, endBlk)
	}
}

func TestChunkManualValues(t *testing.T) {
	startResults := map[int]uint64{0: 67493015, 1: 67493266, 2: 67493517, 3: 67493768}
	endResults := map[int]uint64{0: 67493265, 1: 67493516, 2: 67493767, 3: 67493896}

	events := ChunkEventRangeQuery(250, startResults[0], endResults[3], "event_name")
	if len(events) != len(startResults) {
		t.Errorf("chunks received: %d should have been %d", len(events), len(startResults))
	}
	for index, e := range events {
		assertBlockRange(t, e, 250)
		assertBlockValues(t, e, startResults[index], endResults[index])
	}
}

func TestChunkSmallEndSegments(t *testing.T) {
	startResults := map[int]uint64{0: 0, 1: 51, 2: 102, 3: 153}
	endResults := map[int]uint64{0: 50, 1: 101, 2: 152, 3: 155}

	events := ChunkEventRangeQuery(50, startResults[0], endResults[3], "event_name")
	if len(events) != len(startResults) {
		t.Errorf("chunks received: %d should have been %d", len(events), len(startResults))
	}
	for index, e := range events {
		assertBlockRange(t, e, 50)
		assertBlockValues(t, e, startResults[index], endResults[index])
	}
}

func assertBlockRange(t *testing.T, event client.EventRangeQuery, maxRange uint64) {
	if event.EndHeight-event.StartHeight > maxRange {
		t.Errorf("block range exceeded: %d", event.EndHeight-event.StartHeight)
	}
}
func assertBlockValues(t *testing.T, event client.EventRangeQuery, start, end uint64) {
	if event.StartHeight != start {
		t.Errorf("start height incorrect: %d should have been %d", event.StartHeight, start)
	}
	if event.EndHeight != end {
		t.Errorf("end height incorrect: %d should have been %d", event.EndHeight, end)
	}
}
