package notsnorlax

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	pb "github.com/xdefrag/yaw/testdata/event"
)

// docker run -p 5672:5672 rabbitmq:3.8-alpine

func TestPubSub(t *testing.T) {
	assert := assert.New(t)

	q := &Client{}
	defer q.Close()

	event := &pb.Event{
		Type:    "test",
		Content: "Hello",
	}

	var wg sync.WaitGroup

	wg.Add(100)

	consumerInit := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())

	queue := "queue.test.pubsub"

	go func() {
		assert.NoError(q.Consume(ctx, queue, func(ctx context.Context, msg proto.Message) error {
			ev, ok := msg.(*pb.Event)

			assert.True(ok, "Wrong proto format")
			assert.Equal(event, ev, "Protos not equal")

			wg.Done()

			return nil
		}))

		assert.NoError(q.QueuePurge(queue))

		close(consumerInit)
	}()

	<-consumerInit

	for i := 1; i <= 100; i++ {
		go assert.NoError(q.Publish(context.Background(), queue, event, PublishPersistent()))
		go assert.NoError(q.IsClosed())
	}

	wg.Wait()

	// Консьюмер должен быть обязательно красиво потушен с помощью контекста,
	// а то будет негодовать.
	cancel()

	time.Sleep(time.Second)

	if err := q.QueueDelete(queue); err != nil {
		t.Fatal(err)
	}
}

func TestReqRep(t *testing.T) {
	assert := assert.New(t)

	q := &Client{}
	defer q.Close()

	event := &pb.Event{
		Type: "test",
	}

	ctx, cancel := context.WithCancel(context.Background())

	queue := "queue.test.rpc"

	go func() {
		assert.NoError(q.Consume(ctx, queue, func(ctx context.Context, msg proto.Message) error {
			hh := FromContext(ctx)

			assert.Contains(hh, "ReplyTo")
			assert.Contains(hh, "CorrelationID")

			replyTo := hh["ReplyTo"].(string)
			correlationID := hh["CorrelationID"].(string)

			ev, ok := msg.(*pb.Event)

			assert.True(ok)

			ev.Content = "Hello"

			q.Publish(ctx, replyTo, ev, PublishCorrelationID(correlationID))

			return nil
		},
			ConsumeAutoAck(),
			ConsumeAutoDelete(),
		))
	}()

	var wg sync.WaitGroup

	wg.Add(100)

	for i := 1; i <= 100; i++ {
		replyTo := fmt.Sprintf("queue.reply.%d", i)
		corrID := fmt.Sprintf("test%d", i)

		go assert.NoError(q.Consume(
			ctx,
			replyTo,
			func(ctx context.Context, msg proto.Message) error {
				ev, ok := msg.(*pb.Event)

				assert.True(ok)
				assert.Equal("Hello", ev.Content)

				wg.Done()

				time.Sleep(time.Millisecond)

				return nil
			},
			ConsumeAutoDelete(),
			ConsumeAutoAck(),
		))

		go assert.NoError(q.Publish(context.Background(),
			queue,
			event,
			PublishReplyTo(replyTo),
			PublishCorrelationID(corrID),
		))

		go assert.NoError(q.IsClosed())
	}

	wg.Wait()

	cancel()

	// Ожидание удаления очередей.
	time.Sleep(time.Second)
}

func TestReqRepWithPublishAndConsume(t *testing.T) {
	assert := assert.New(t)

	q := &Client{}
	defer q.Close()

	event := &pb.Event{
		Type: "test",
	}

	ctx, cancel := context.WithCancel(context.Background())

	queue := "queue.test.pc"

	go assert.NoError(q.Consume(ctx, queue, func(ctx context.Context, msg proto.Message) error {
		hh := FromContext(ctx)

		assert.Contains(hh, "ReplyTo")

		replyTo := hh["ReplyTo"].(string)

		ev, ok := msg.(*pb.Event)

		assert.True(ok)

		ev.Content = "Hello"

		q.Publish(ctx, replyTo, ev)

		return nil
	},
		ConsumeAutoAck(),
		ConsumeAutoDelete(),
	))

	var wg sync.WaitGroup

	wg.Add(100)

	for i := 1; i <= 100; i++ {
		got, err := q.PublishAndConsume(context.Background(), queue, event)

		assert.NoError(err)

		want := &pb.Event{
			Type:    "test",
			Content: "Hello",
		}

		assert.Equal(want, got)

		wg.Done()

		go assert.NoError(q.IsClosed())
	}

	wg.Wait()

	cancel()

	time.Sleep(time.Second)
}

func TestReqRepErrors(t *testing.T) {
	assert := assert.New(t)

	q := &Client{}
	defer q.Close()

	event := &pb.Event{
		Type: "test",
	}

	ctx, cancel := context.WithCancel(context.Background())

	queue := "queue.test.pce"

	go assert.NoError(q.Consume(ctx, queue, func(ctx context.Context, msg proto.Message) error {
		return nil
	},
		ConsumeAutoAck(),
		ConsumeAutoDelete(),
	))

	var wg sync.WaitGroup

	wg.Add(100)

	for i := 1; i <= 100; i++ {
		go func() {
			got, err := q.PublishAndConsume(context.Background(), queue, event, PublishAndConsumeTimeout(time.Millisecond))

			assert.Nil(got)
			assert.Error(err)

			wg.Done()
		}()

		go assert.NoError(q.IsClosed())
	}

	wg.Wait()

	cancel()

	time.Sleep(time.Second)
}
