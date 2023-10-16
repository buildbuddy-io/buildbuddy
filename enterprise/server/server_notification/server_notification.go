// server_notification provides a best-effort pubsub-style notification service
// for broadcasting information to other servers within the same service.
//
// Payloads are defined by adding a new proto to the Notification parent
// proto in the server_notification proto file.
package server_notification

import (
	"context"
	"encoding/base64"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pubsub"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	snpb "github.com/buildbuddy-io/buildbuddy/proto/server_notification"
)

const (
	baseChannelName = "serverNotifications/"
)

type Service struct {
	ps          *pubsub.PubSub
	channelName string

	msgTypes map[protoreflect.MessageDescriptor]protoreflect.FieldDescriptor

	mu   sync.Mutex
	subs map[protoreflect.MessageDescriptor][]chan<- proto.Message
}

func Register(env environment.Env, serviceName string) error {
	rdb := env.GetDefaultRedisClient()
	if rdb == nil {
		return nil
	}

	env.SetServerNotificationService(New(serviceName, rdb))
	return nil
}

func New(serviceName string, rdb redis.UniversalClient) *Service {
	ps := pubsub.NewPubSub(rdb)

	msgTypes := make(map[protoreflect.MessageDescriptor]protoreflect.FieldDescriptor)
	pfs := (&snpb.Notification{}).ProtoReflect().Descriptor().Fields()
	for i := 0; i < pfs.Len(); i++ {
		pf := pfs.Get(i)
		msgTypes[pf.Message()] = pf
	}

	channelName := baseChannelName + serviceName
	svc := &Service{
		ps:          ps,
		channelName: channelName,
		msgTypes:    msgTypes,
		subs:        make(map[protoreflect.MessageDescriptor][]chan<- proto.Message),
	}
	go func() {
		sub := ps.Subscribe(context.Background(), channelName)
		for msg := range sub.Chan() {
			svc.dispatch(msg)
		}
	}()
	return svc
}

func (s *Service) dispatch(msg string) {
	pbMsg := snpb.Notification{}
	bs, err := base64.StdEncoding.DecodeString(msg)
	if err != nil {
		alert.UnexpectedEvent("invalid_app_notification_msg", "could not decode message: %s", err)
		return
	}
	if err := proto.Unmarshal(bs, &pbMsg); err != nil {
		alert.UnexpectedEvent("invalid_app_notification_msg", "could not unmarshal message: %s", err)
		return
	}

	pbMsg.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Kind() == protoreflect.MessageKind {
			s.mu.Lock()
			chs := make([]chan<- proto.Message, len(s.subs[field.Message()]))
			copy(chs, s.subs[field.Message()])
			s.mu.Unlock()
			go func() {
				for _, ch := range chs {
					ch <- value.Message().Interface()
				}
			}()
		}
		return true
	})
}

func (s *Service) Subscribe(msgType proto.Message) <-chan proto.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan proto.Message)
	dsc := msgType.ProtoReflect().Descriptor()
	s.subs[dsc] = append(s.subs[dsc], ch)
	return ch
}

func (s *Service) Publish(ctx context.Context, msg proto.Message) error {
	fd, ok := s.msgTypes[msg.ProtoReflect().Descriptor()]
	if !ok {
		alert.UnexpectedEvent("invalid_app_notification_msg_type", "could not set message: %s", msg.ProtoReflect().Descriptor())
		return status.InvalidArgumentErrorf("invalid msg proto: %s", msg.ProtoReflect().Descriptor())
	}

	n := &snpb.Notification{}
	n.ProtoReflect().Set(fd, protoreflect.ValueOfMessage(msg.ProtoReflect()))

	bs, err := proto.Marshal(n)
	if err != nil {
		return err
	}
	return s.ps.Publish(ctx, s.channelName, base64.StdEncoding.EncodeToString(bs))
}
