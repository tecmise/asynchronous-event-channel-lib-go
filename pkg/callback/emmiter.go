package callback

import (
	"context"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/definition"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/emitter"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/properties"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/publisher"
	"gorm.io/gorm"
	"reflect"
	"strings"
)

type AnyChannel[R any] interface {
	OnUpdate(ctx context.Context, req R, metadata definition.EmitableMetadata, properties properties.FifoProperties) (*publisher.SnsTriggerResponse, error)
	OnDelete(ctx context.Context, req R, metadata definition.EmitableMetadata, properties properties.FifoProperties) (*publisher.SnsTriggerResponse, error)
	OnCreate(ctx context.Context, req R, metadata definition.EmitableMetadata, properties properties.FifoProperties) (*publisher.SnsTriggerResponse, error)
}

type adapter[R any] struct {
	Ch        emitter.Channel[R]
	Validator func(R) error
}

type channelAdapter[E any, R any] struct {
	adapter            adapter[R]
	EntityReflectType  reflect.Type
	RequestReflectType reflect.Type
}

func WrapperChannel[E any, R any](ch emitter.Channel[R]) emitter.Channel[any] {
	e := reflect.TypeOf(new(E)).Elem()
	r := reflect.TypeOf(new(R)).Elem()
	return &channelAdapter[E, R]{
		adapter: adapter[R]{
			Ch:        ch,
			Validator: nil,
		},
		EntityReflectType:  e,
		RequestReflectType: r,
	}
}

func WrapperChannelWithValidation[E any, R any](ch emitter.Channel[R], validation func(R) error) emitter.Channel[any] {
	adp := WrapperChannel[E, R](ch).(*channelAdapter[E, R])
	adp.adapter.Validator = validation
	return adp
}

func (a *channelAdapter[E, R]) convertReq(req any) (R, error) {
	var zero R
	if typed, ok := req.(R); ok {
		return typed, nil
	}
	rv := reflect.ValueOf(req)
	if rv.IsValid() && rv.Kind() == reflect.Ptr && !rv.IsNil() {
		elem := rv.Elem()
		if elem.IsValid() && elem.CanInterface() {
			if val, ok := elem.Interface().(R); ok {
				return val, nil
			}
		}
	}
	rv = reflect.ValueOf(req)
	if rv.IsValid() && rv.Kind() != reflect.Ptr {
		if reflect.TypeOf(zero).Kind() == reflect.Ptr {
			if rv.Type().AssignableTo(reflect.TypeOf(zero).Elem()) {
				ptr := reflect.New(rv.Type())
				ptr.Elem().Set(rv)
				if val, ok := ptr.Interface().(R); ok {
					return val, nil
				}
			}
		}
	}
	return zero, errors.New("request type mismatch in channel adapter")
}

func (a *channelAdapter[E, R]) toTypedEmitable(em definition.Emitable[any]) (definition.Emitable[E], error) {
	if typed, ok := em.(definition.Emitable[E]); ok {
		return typed, nil
	}
	return nil, errors.New("emitable type mismatch")
}

func (a *channelAdapter[E, R]) OnUpdate(ctx context.Context, req any, metadata definition.EmitableMetadata, properties properties.FifoProperties) (*publisher.SnsTriggerResponse, error) {
	typedReq, err := a.convertReq(req)
	if err != nil {
		return nil, err
	}

	if a.adapter.Validator != nil {
		val := a.adapter.Validator(typedReq)
		if val != nil {
			logrus.Errorf("validation error on OnUpdate: %v", val)
			return nil, val
		}
	}

	return a.adapter.Ch.OnUpdate(ctx, typedReq, metadata, properties)
}

func (a *channelAdapter[E, R]) OnDelete(ctx context.Context, req any, metadata definition.EmitableMetadata, properties properties.FifoProperties) (*publisher.SnsTriggerResponse, error) {
	typedReq, err := a.convertReq(req)
	if err != nil {
		return nil, err
	}

	if a.adapter.Validator != nil {
		val := a.adapter.Validator(typedReq)
		if val != nil {
			logrus.Errorf("validation error on OnDelete: %v", val)
			return nil, val
		}
	}

	return a.adapter.Ch.OnDelete(ctx, typedReq, metadata, properties)
}

func (a *channelAdapter[E, R]) OnCreate(ctx context.Context, req any, metadata definition.EmitableMetadata, properties properties.FifoProperties) (*publisher.SnsTriggerResponse, error) {
	typedReq, err := a.convertReq(req)
	if err != nil {
		return nil, err
	}
	if a.adapter.Validator != nil {
		val := a.adapter.Validator(typedReq)
		if val != nil {
			logrus.Errorf("validation error on OnCreate: %v", val)
			return nil, val
		}
	}

	return a.adapter.Ch.OnCreate(ctx, typedReq, metadata, properties)
}

func NewAsyncChannel() AsyncChannel {
	return &asyncChannel{
		channels: make(map[string]emitter.Channel[any]),
	}
}

type AsyncChannel interface {
	AddChannels(channel ...emitter.Channel[any])
	RegistryEmit(db *gorm.DB) error
}
type asyncChannel struct {
	channels map[string]emitter.Channel[any]
}

func (a *asyncChannel) RegistryEmit(db *gorm.DB) error {
	errInsert := db.Callback().Create().After("gorm:create").Register("emit_create", func(db *gorm.DB) {
		err := a.emit(db, "INSERT")
		if err != nil {
			logrus.WithError(err).Error("error on insert database")
		}
	})

	if errInsert != nil {
		logrus.Error("error to registry callback to insert")
		return errInsert
	}

	errUpdate := db.Callback().Create().After("gorm:update").Register("emit_update", func(db *gorm.DB) {
		err := a.emit(db, "UPDATE")
		if err != nil {
			logrus.WithError(err).Error("error on update database")
		}
	})

	if errUpdate != nil {
		logrus.Error("error to registry callback to update")
		return errUpdate
	}

	errDelete := db.Callback().Create().After("gorm:delete").Register("emit_delete", func(db *gorm.DB) {
		err := a.emit(db, "DELETE")
		if err != nil {
			logrus.WithError(err).Error("error on delete database")
		}
	})

	if errDelete != nil {
		logrus.Error("error to registry callback to delete")
		return errInsert
	}

	return nil

}
func (a *asyncChannel) AddChannels(channels ...emitter.Channel[any]) {

	if channels == nil || len(channels) == 0 {
		logrus.Error("no channels provided to add")
		return
	}

	for _, channel := range channels {
		if a.channels == nil {
			a.channels = make(map[string]emitter.Channel[any])
		}

		rv := reflect.ValueOf(channel)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}

		if rv.Kind() != reflect.Struct {
			logrus.Error("channel is not a struct")
			return
		}

		field := rv.FieldByName("EntityReflectType")
		if !field.IsValid() {
			logrus.Error("field ReflectedType not found on channel")
			return
		}

		typ, ok := field.Interface().(reflect.Type)
		if !ok {
			logrus.Error("EntityReflectType has unexpected type")
			return
		}

		key := typ.String()
		if strings.HasSuffix(key, "*") {
			a.channels[key] = channel
		} else {
			a.channels[fmt.Sprintf("*%s", key)] = channel
		}

	}

}

func (a *asyncChannel) emit(db *gorm.DB, operation string) error {
	obj := db.Statement.Dest

	key := reflect.TypeOf(obj).String()

	rv := reflect.ValueOf(obj)
	if !rv.IsValid() {
		return nil
	}

	// chamada Metadada()
	metaMethod := rv.MethodByName("Metadada")
	if !metaMethod.IsValid() {
		// não implementa Metadada(), pula
		return nil
	}
	metaOut := metaMethod.Call(nil)
	if len(metaOut) != 1 {
		return nil
	}
	metadata, ok := metaOut[0].Interface().(definition.EmitableMetadata)
	if !ok {
		return nil
	}

	// chamada GetAsyncEmitterData()
	getMethod := rv.MethodByName("GetAsyncEmitterData")
	if !getMethod.IsValid() {
		return nil
	}
	out := getMethod.Call(nil)
	if len(out) < 2 {
		return nil
	}

	// segundo retorno é error
	var err error
	if !out[1].IsNil() {
		if e, ok := out[1].Interface().(error); ok {
			err = e
		}
	}
	if err != nil {
		logrus.Errorf("error getting async emitter data: %v", err)
		return err
	}

	dto := out[0].Interface()
	logrus.WithField("metadata", metadata).WithField("dto", dto).WithField("key", key).Debug("got async emitter data")

	// chamada GetFifoProperties()
	var fifo *properties.FifoProperties
	fifoMethod := rv.MethodByName("GetFifoProperties")
	if fifoMethod.IsValid() {
		fifoOut := fifoMethod.Call(nil)
		if len(fifoOut) == 1 && !fifoOut[0].IsNil() {
			if f, ok := fifoOut[0].Interface().(*properties.FifoProperties); ok {
				fifo = f
			}
		}
	}
	logrus.Debugf("fifo properties (via reflection): %+v", fifo)
	fields := map[string]interface{}{
		"table":     db.Statement.Table,
		"publisher": metadata.Publisher,
		"name":      metadata.Name,
		"key":       key,
	}
	logrus.WithFields(fields).Debug("Emitting entity")

	channel, hasChannel := a.channels[key]

	if !hasChannel {
		logrus.WithFields(fields).Debug("No channel found for entity, skipping emit")
		return nil
	}

	var result *publisher.SnsTriggerResponse

	if err != nil {
		logrus.Errorf("error getting async emitter data: %v", err)
		return err
	}

	// trate fifo nil e passe dto diretamente (channel espera any)
	props := properties.FifoProperties{}
	if fifo != nil {
		props = *fifo
	}

	if operation == "DELETE" {
		logrus.WithFields(fields).Debug("Deleting entity")
		result, err = channel.OnDelete(db.Statement.Context, dto, metadata, props)
	} else if operation == "INSERT" {
		logrus.WithFields(fields).Debug("Inserting entity")
		result, err = channel.OnCreate(db.Statement.Context, dto, metadata, props)
	} else if operation == "UPDATE" {
		logrus.WithFields(fields).Debug("Updating entity")
		result, err = channel.OnUpdate(db.Statement.Context, dto, metadata, props)
	}
	if err != nil {
		logrus.Errorf("error emitting entity: %v", err)
		return err
	}
	if result == nil {
		return errors.New("error emitting entity: no result")
	}
	logrus.WithField("message_id", result.MessageId).Debug("emited entity")
	return nil
}
