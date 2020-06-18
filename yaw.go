package notsnorlax

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/gofrs/uuid"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/streadway/amqp"
)

const (
	// envAMQPURL Ссылка на amqp вида "https://guest:guest@rabbitmq:5672/".
	envAMQPURL = "AMQP_URL"

	// envAMQPChannelMax Максимально создаваемые каналы на подключении.
	envAMQPChannelMax = "AMQP_CHANNEL_MAX"

	// envAQMPExchange Используемый обменник. По умолчанию не используется.
	envAMQPExchange = "AMQP_EXCHANGE"

	// envAMQPQOS Количество сообщений взятых перех паузой в 200мс.
	envAMQPQOS = "AMQP_QOS"

	// envAMQPSource Подписывание отправителя в хидере при публикации сообщений.
	envAMQPSource = "AMQP_SOURCE"
)

var (
	errSubNoMessageType      = errors.New("no message type")
	errSubUnknownMessageType = errors.New("unknown message type")
	errSubUnmarshaling       = errors.New("failed to unmarshal message")
)

type Client struct {
	conn   *amqp.Connection
	ch     *amqp.Channel
	logger logr.InfoLogger
}

// SetLogger Устанавливает логгер для событий внутри библиотеки.
// Рекомендуется использовать для дебага.
func (c *Client) SetLogger(logger logr.InfoLogger) {
	c.logger = logger
}

// IsClosed Выдает ошибку amqp.ErrClosed, если коннекшн до RMQ закрылся.
func (c *Client) IsClosed() error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}

	if conn.IsClosed() {
		return amqp.ErrClosed
	}

	return nil
}

func (c *Client) info(msg string, kv ...interface{}) {
	if c.logger != nil {
		c.logger.Info(msg, kv...)
	}
}

func (c *Client) getConnection() (*amqp.Connection, error) {
	if c.conn == nil || c.conn.IsClosed() {
		url := getenv(envAMQPURL, "amqp://guest:guest@0.0.0.0:5672/")
		max := getenvi(envAMQPChannelMax, 5)

		c.info("Init connection", "url", strings.TrimLeft(url, "@"), "channelMax", max)

		conn, err := amqp.DialConfig(
			url,
			amqp.Config{
				ChannelMax:      max,
				TLSClientConfig: &tls.Config{},
			},
		)

		if err != nil {
			return nil, err
		}

		c.conn = conn
	}

	return c.conn, nil
}

func (c *Client) getChannel() (*amqp.Channel, error) {
	if c.ch != nil {
		return c.ch, nil
	}

	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if err := ch.Qos(getenvi(envAMQPQOS, 1), 0, false); err != nil {
		return nil, err
	}

	c.ch = ch

	return c.ch, nil
}

var key = &struct{}{} // nolint

// Headers Тип заголовков для сообщений amqp.
type Headers map[string]interface{}

// FromContext Достает заголовки из контекста. Рекомендуется использовать
// только для дебага и логирования.
func FromContext(ctx context.Context) Headers {
	return ctx.Value(key).(Headers)
}

// ToContext Создает контекст с заголовками для отправки.
func ToContext(ctx context.Context, h Headers) context.Context {
	return context.WithValue(ctx, key, h)
}

type PublishOptionFn func(*amqp.Publishing)

// PublishReplyTo Установка параметра ReplyTo в сообщение.
func PublishReplyTo(queue string) PublishOptionFn {
	return func(p *amqp.Publishing) {
		p.ReplyTo = queue
	}
}

// PublishCorrelationID Установка соответствующего заголовка.
func PublishCorrelationID(id string) PublishOptionFn {
	return func(p *amqp.Publishing) {
		p.CorrelationId = id
	}
}

// PublishPersistent установка персистентных сообщений: DeliveryMode = 2.
func PublishPersistent() PublishOptionFn {
	return func(p *amqp.Publishing) {
		p.DeliveryMode = 2
	}
}

// Publish Публикация сообщений.
func (c *Client) Publish(ctx context.Context, queue string, msg proto.Message, optFns ...PublishOptionFn) error {
	ch, err := c.getChannel()
	if err != nil {
		return err
	}

	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	msgt := proto.MessageName(msg)

	publ := amqp.Publishing{
		Headers: amqp.Table{
			"MessageType": msgt,
			"Source":      getenv(envAMQPSource, "NOTSNORLAX"),
		},
		ContentType: "application/protobuf",
		Body:        body,
	}

	for _, fn := range optFns {
		fn(&publ)
	}

	c.info(
		"Publishing",
		"queue", queue,
		"messageType", msgt,
		"replyTo", publ.ReplyTo,
		"correlationID", publ.CorrelationId,
	)

	return ch.Publish(
		getenv(envAMQPExchange, ""),
		queue,
		false,
		false,
		publ,
	)
}

// Consumer Функция для принятия сообщений.
type Consumer func(ctx context.Context, msg proto.Message) error

type ConsumeOptions struct {
	autoAck    bool
	exclusive  bool
	autoDelete bool
	durable    bool

	once bool
}

type ConsumeOptionFn func(*ConsumeOptions)

// ConsumeAutoAck Отключает необходимость подтверждения сообщений.
func ConsumeAutoAck() ConsumeOptionFn {
	return func(o *ConsumeOptions) {
		o.autoAck = true
	}
}

// ConsumeExclusive Устанавливает эксклюзивность очереди.
// Использовать только, если знаешь, что делаешь. Если необходимо
// удалить очередь после использования, лучше использовать ConsumeAutoDelete.
func ConsumeExclusive() ConsumeOptionFn {
	return func(o *ConsumeOptions) {
		o.exclusive = true
	}
}

// ConsumeAutoDelete удаляет очередь после закрытия канала. Дополнительно
// при окончании контекста (ctx.Done) удаляет очередь через QueueDelete.
func ConsumeAutoDelete() ConsumeOptionFn {
	return func(o *ConsumeOptions) {
		o.autoDelete = true
	}
}

// ConsumeDurable Сохранять данные в очереди, даже если отсутствуют потребители.
func ConsumeDurable() ConsumeOptionFn {
	return func(o *ConsumeOptions) {
		o.durable = true
	}
}

// ConsumeOnce Потребить один раз, затем закончить контекст.
func ConsumeOnce() ConsumeOptionFn {
	return func(o *ConsumeOptions) {
		o.once = true
	}
}

// Consume Потребление сообщений из очереди.
func (c *Client) Consume(ctx context.Context, queue string, h Consumer, optsFn ...ConsumeOptionFn) error {
	ch, err := c.getChannel()
	if err != nil {
		return err
	}

	opts := ConsumeOptions{}

	for _, fn := range optsFn {
		fn(&opts)
	}

	_, err = ch.QueueDeclare(
		queue,
		opts.durable,    // durable
		opts.autoDelete, // autoDelete
		opts.exclusive,  // exclusive
		false,           // noWait
		amqp.Table{},
	)

	if err != nil {
		return err
	}

	exchange := getenv(envAMQPExchange, "")

	if exchange != "" {
		if err := ch.QueueBind(
			queue,
			queue,
			exchange,
			false,
			amqp.Table{},
		); err != nil {
			return err
		}
	}

	chd, err := ch.Consume(
		queue,
		"",
		opts.autoAck,   // autoAck
		opts.exclusive, // exclusive
		false,          // noLocal
		false,          // noWait
		amqp.Table{},
	)

	if err != nil {
		return err
	}

	go c.consumerLoop(ctx, opts, chd, h)

	return nil
}

func (c *Client) consumerLoop(ctx context.Context, opts ConsumeOptions, chd <-chan amqp.Delivery, h Consumer) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		case d := <-chd:
			id := uuid.Must(uuid.NewV4()).String()

			messageType := iToString(d.Headers["MessageType"])
			contentType := iToString(d.ContentType)

			kv := []interface{}{
				"id", id,
				"messageType", messageType,
				"contentType", contentType,
				"replyTo", d.ReplyTo,
				"correlationID", d.CorrelationId,
			}

			c.info("Consumed", kv...)

			msg, err := decodeMsgToProto(
				contentType,
				messageType,
				d.Body,
			)

			if err != nil {
				kv = append(kv, "error", err.Error())

				c.info("Invalid message", kv...)

				ack(cancel, d, true, opts.autoAck, opts.once)

				continue
			}

			d.Headers["CorrelationID"] = d.CorrelationId
			d.Headers["ReplyTo"] = d.ReplyTo

			ctxh := ToContext(context.Background(), Headers(d.Headers))

			if err := h(ctxh, msg); err != nil {
				c.info("Rejected", kv...)

				ack(cancel, d, true, opts.autoAck, opts.once)

				continue
			}

			c.info("Acked", kv...)

			ack(cancel, d, false, opts.autoAck, opts.once)
		}
	}
}

func ack(cancel context.CancelFunc, d amqp.Delivery, reject bool, autoAck, once bool) {
	if !autoAck && reject {
		_ = d.Reject(false)
	}

	if !autoAck && !reject {
		_ = d.Ack(false)
	}

	if once {
		cancel()
	}
}

type PublishAndConsumeOptions struct {
	timeout time.Duration
}

type PublishAndConsumeOptionFn func(*PublishAndConsumeOptions)

// PublishAndConsumeTimeout Время ожидания ответа из очереди. По умолчанию 1 секунда.
func PublishAndConsumeTimeout(t time.Duration) PublishAndConsumeOptionFn {
	return func(o *PublishAndConsumeOptions) {
		o.timeout = t
	}
}

// PublishAndConsume Фасад для публикации и потребления (читай как RPC). Отправляет сообщение
// в очередь, ожидает ответа по ReplyTo и возвращает из функции.
func (c *Client) PublishAndConsume(ctx context.Context, queue string,
	msg proto.Message, optFns ...PublishAndConsumeOptionFn) (proto.Message, error) {
	opts := PublishAndConsumeOptions{
		timeout: time.Second,
	}

	for _, fn := range optFns {
		fn(&opts)
	}

	resc := make(chan proto.Message)

	corrID := uuid.Must(uuid.NewV4()).String()

	h := func(ctx context.Context, msg proto.Message) error {
		select {
		case <-ctx.Done():
		default:
			resc <- msg
		}

		return nil
	}

	replyTo := uuid.Must(uuid.NewV4()).String()

	ctx, cancel := context.WithTimeout(ctx, opts.timeout)
	defer cancel()

	if err := c.Consume(ctx, replyTo, h, ConsumeOnce(), ConsumeAutoAck(), ConsumeAutoDelete()); err != nil {
		return nil, err
	}

	if err := c.Publish(ctx, queue, msg, PublishReplyTo(replyTo), PublishCorrelationID(corrID)); err != nil {
		return nil, err
	}

	select {
	case <-time.NewTimer(opts.timeout).C:
		close(resc)

		return nil, fmt.Errorf("timeout exceeded")
	case res := <-resc:
		close(resc)

		return res, nil
	}
}

// QueueDelete Удаление очереди.
func (c *Client) QueueDelete(queue string) error {
	ch, err := c.getChannel()
	if err != nil {
		return err
	}

	_, err = ch.QueueDelete(queue, false, false, false)

	return err
}

// QueuePurge Отчистка очереди.
func (c *Client) QueuePurge(queue string) error {
	ch, err := c.getChannel()
	if err != nil {
		return err
	}

	_, err = ch.QueuePurge(queue, false)

	return err
}

// Close закрывает подключение.
func (c *Client) Close() error {
	return c.conn.Close()
}

func getenv(k, d string) string {
	if r := os.Getenv(k); r != "" {
		return r
	}

	return d
}

func getenvi(k string, d int) int {
	if r := os.Getenv(k); r != "" {
		rr, _ := strconv.Atoi(r)

		return rr
	}

	return d
}

func decodeMsgToProto(contentType, messageType string, b []byte) (proto.Message, error) {
	if messageType == "" {
		return nil, errSubNoMessageType
	}

	typ := proto.MessageType(messageType)

	if typ == nil {
		return nil, errSubUnknownMessageType
	}

	p := getProtoPtr(typ)

	switch contentType {
	case "":
		fallthrough
	case "application/json":
		if err := jsonpb.Unmarshal(bytes.NewReader(b), p); err != nil {
			return nil, errSubUnmarshaling
		}
	case "application/protobuf":
		if err := proto.Unmarshal(b, p); err != nil {
			return nil, errSubUnmarshaling
		}
	}

	return p, nil
}

func getProtoPtr(t reflect.Type) proto.Message {
	return reflect.New(t.Elem()).Interface().(proto.Message)
}

func iToString(i interface{}) string {
	s, ok := i.(string)

	if !ok {
		return ""
	}

	return s
}
