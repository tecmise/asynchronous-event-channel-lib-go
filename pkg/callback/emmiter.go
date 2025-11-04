package callback

import (
	"context"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/emitter"
	"github.com/tecmise/connector-lib/pkg/adapters/outbound/shared_kernel"
	"github.com/tecmise/connector-lib/pkg/ports/output/assync"
	"github.com/tecmise/connector-lib/pkg/ports/output/request"
	"gorm.io/gorm"
	"reflect"
	"strings"
)

type AnyChannel[R any] interface {
	OnUpdate(ctx context.Context, req R, metadata emitter.EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error)
	OnDelete(ctx context.Context, req R, metadata emitter.EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error)
	OnCreate(ctx context.Context, req R, metadata emitter.EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error)
}

type channelAdapter[E any, R any] struct {
	Ch                     emitter.Channel[R]
	EntityReflectType      reflect.Type
	RequestReflectType     reflect.Type
	Validations            []request.CustomValidator
	RImplementsValidatable bool
}

func WrapperChannel[E any, R any](ch emitter.Channel[R]) emitter.Channel[any] {
	e := reflect.TypeOf(new(E)).Elem()
	r := reflect.TypeOf(new(R)).Elem()
	validType := reflect.TypeOf((*request.Validatable)(nil)).Elem()
	hasValid := r.Implements(validType) || reflect.PtrTo(r).Implements(validType)

	return &channelAdapter[E, R]{
		Ch:                     ch,
		EntityReflectType:      e,
		RequestReflectType:     r,
		Validations:            nil,
		RImplementsValidatable: hasValid,
	}
}

func WrapperChannelWithValidations[E any, R any](ch emitter.Channel[R], validations ...request.CustomValidator) emitter.Channel[any] {
	adapter := WrapperChannel[E, R](ch).(*channelAdapter[E, R])
	adapter.Validations = validations
	return adapter
}

func (a *channelAdapter[E, R]) getValidatable(typedReq R) (request.Validatable, bool) {
	if v, ok := any(typedReq).(request.Validatable); ok {
		return v, true
	}

	rv := reflect.ValueOf(typedReq)
	if !rv.IsValid() {
		return nil, false
	}
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil, false
		}
		if v, ok := rv.Interface().(request.Validatable); ok {
			return v, true
		}
		return nil, false
	}

	ptr := reflect.New(rv.Type())
	ptr.Elem().Set(rv)
	if v, ok := ptr.Interface().(request.Validatable); ok {
		return v, true
	}

	if rv.CanAddr() {
		addr := rv.Addr()
		if v, ok := addr.Interface().(request.Validatable); ok {
			return v, true
		}
	}

	return nil, false
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

func (a *channelAdapter[E, R]) toTypedEmitable(em emitter.Emitable[any]) (emitter.Emitable[E], error) {
	if typed, ok := em.(emitter.Emitable[E]); ok {
		return typed, nil
	}
	return nil, errors.New("emitable type mismatch")
}

func (a *channelAdapter[E, R]) OnUpdate(ctx context.Context, req any, metadata emitter.EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error) {
	typedReq, err := a.convertReq(req)
	if err != nil {
		return nil, err
	}

	vr, ok := a.getValidatable(typedReq)

	if ok {
		if a.RImplementsValidatable && len(a.Validations) > 0 {
			if err := request.ValidateObject(vr, a.Validations...); err != nil {
				return nil, err
			}
		} else {
			if err := request.ValidateObject(vr); err != nil {
				return nil, err
			}
		}
	}

	return a.Ch.OnUpdate(ctx, typedReq, metadata, properties)
}

func (a *channelAdapter[E, R]) OnDelete(ctx context.Context, req any, metadata emitter.EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error) {
	typedReq, err := a.convertReq(req)
	if err != nil {
		return nil, err
	}

	vr, ok := a.getValidatable(typedReq)

	if ok {
		if a.RImplementsValidatable && len(a.Validations) > 0 {
			if err := request.ValidateObject(vr, a.Validations...); err != nil {
				return nil, err
			}
		} else {
			if err := request.ValidateObject(vr); err != nil {
				return nil, err
			}
		}
	}

	return a.Ch.OnDelete(ctx, typedReq, metadata, properties)
}

func (a *channelAdapter[E, R]) OnCreate(ctx context.Context, req any, metadata emitter.EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error) {
	typedReq, err := a.convertReq(req)
	if err != nil {
		return nil, err
	}

	vr, ok := a.getValidatable(typedReq)

	if ok {
		if a.RImplementsValidatable && len(a.Validations) > 0 {
			if err := request.ValidateObject(vr, a.Validations...); err != nil {
				return nil, err
			}
		} else {
			if err := request.ValidateObject(vr); err != nil {
				return nil, err
			}
		}
	}

	return a.Ch.OnCreate(ctx, typedReq, metadata, properties)
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
			logrus.Errorf("error on create database: %v", err)
		}
	})

	if errInsert != nil {
		logrus.Error("error to registry callback to insert")
		return errInsert
	}

	errUpdate := db.Callback().Create().After("gorm:update").Register("emit_update", func(db *gorm.DB) {
		err := a.emit(db, "UPDATE")
		if err != nil {
			logrus.Errorf("error on update database: %v", err)
		}
	})

	if errUpdate != nil {
		logrus.Error("error to registry callback to update")
		return errUpdate
	}

	errDelete := db.Callback().Create().After("gorm:delete").Register("emit_delete", func(db *gorm.DB) {
		err := a.emit(db, "DELETE")
		if err != nil {
			logrus.Errorf("error on delete database: %v", err)
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
	metadata, ok := metaOut[0].Interface().(emitter.EmitableMetadata)
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
	var fifo *shared_kernel.FifoProperties
	fifoMethod := rv.MethodByName("GetFifoProperties")
	if fifoMethod.IsValid() {
		fifoOut := fifoMethod.Call(nil)
		if len(fifoOut) == 1 && !fifoOut[0].IsNil() {
			if f, ok := fifoOut[0].Interface().(*shared_kernel.FifoProperties); ok {
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

	var result *assync.SnsTriggerResponse

	if err != nil {
		logrus.Errorf("error getting async emitter data: %v", err)
		return err
	}

	// trate fifo nil e passe dto diretamente (channel espera any)
	props := shared_kernel.FifoProperties{}
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
