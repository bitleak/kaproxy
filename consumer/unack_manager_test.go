package consumer

import (
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestUnackManager_Heap(t *testing.T) {
	size := 10

	//init
	h := &messageHeap{
		messages:  make([]*unackMessage, 0, size),
		offsetMap: new(sync.Map),
	}

	//push
	for i := 0; i < size; i++ {
		message := &sarama.ConsumerMessage{
			Partition: 0,
			Offset:    int64(i),
		}
		expire := time.Now().Add(time.Duration(i*-10) * time.Millisecond)
		h.push(message, expire)
		if h.Len() != i+1 {
			t.Fatalf("Failed to push message to heap")
		}
	}

	//sort and pop
	for i := 0; i < size; i++ {
		message := h.pop()
		if message.Offset != int64(size-1-i) {
			t.Fatalf("Failed to pop message from heap")
		}
	}

	//remove
	for i := 0; i < size; i++ {
		message := &sarama.ConsumerMessage{
			Partition: 0,
			Offset:    int64(i),
		}
		expire := time.Now().Add(time.Duration(i*-10) * time.Millisecond)
		h.push(message, expire)
	}
	if err := h.remove(0, int64(size)); err != ErrUnackMessageNotFound {
		t.Fatalf("expect %s, but got %s", ErrUnackMessageNotFound, err)
	}
	for i := size / 2; i < size; i++ {
		if err := h.remove(0, int64(i)); err != nil {
			t.Fatalf("Failed to remove message from heap")
		}
	}
	for i := size/2 - 1; i >= 0; i-- {
		message := h.pop()
		if message.Offset != int64(i) {
			t.Fatalf("Failed to pop message from heap after remove some message")
		}
	}
}

func TestUnackManager_Offsets(t *testing.T) {
	size := 10
	type offset struct {
		partition int32
		offset    int64
	}
	offsets := []offset{
		{0, 0},
		{0, 1},
		{0, 2},
		{0, 3},
		{0, 4},
		{0, 5},
		{0, 6},
		{1, 0},
		{1, 1},
		{2, 0},
	}

	type ack struct {
		partition int32
		offset    int64
		len       int
	}
	acks := []ack{
		{0, 5, 10},
		{0, 3, 10},
		{0, 6, 10},
		{0, 0, 9},
		{2, 0, 8},
		{1, 1, 8},
		{1, 0, 6},
		{0, 2, 6},
		{0, 1, 3},
		{0, 4, 0},
	}

	partitions := make([]int32, 0, 3)
	for i := 0; i < 3; i++ {
		partitions = append(partitions, int32(i))
	}
	o := newOffsets(partitions, size)
	// enqueue

	for i := 0; i < size; i++ {
		select {
		case <-o.nonFull:
		default:
			t.Fatalf("Failed to enqueue message")
		}
		o.enqueue(offsets[i].partition, offsets[i].offset)
	}

	//check if queue is full
	select {
	case <-o.nonFull:
		t.Fatalf("Failed to check if queue is full")
	default:
	}

	//dequeue
	for i := 0; i < size; i++ {
		o.dequeue(acks[i].partition, acks[i].offset)
		if o.len != acks[i].len {
			t.Fatalf("Failed to dequeue message")
		}
		if o.len == size {
			select {
			case <-o.nonFull:
				t.Fatalf("Failed to check if queue if full")
			default:
			}
		} else {
			select {
			case <-o.nonFull:
			default:
				t.Fatalf("Failed to reopen nonFull channel")
			}
		}
	}
}
