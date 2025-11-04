package callback

import (
	"context"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/emitter"
	"github.com/tecmise/connector-lib/pkg/adapters/outbound/shared_kernel"
	"github.com/tecmise/connector-lib/pkg/ports/output/assync"
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
	Ch                 emitter.Channel[R]
	EntityReflectType  reflect.Type
	RequestReflectType reflect.Type
}

func WrapperChannel[E any, R any](ch emitter.Channel[R]) emitter.Channel[any] {
	e := reflect.TypeOf(new(E)).Elem()
	r := reflect.TypeOf(new(R)).Elem()
	return &channelAdapter[E, R]{
		Ch:                 ch,
		EntityReflectType:  e,
		RequestReflectType: r,
	}
}

func (a *channelAdapter[E, R]) convertReq(req any) (R, error) {
	var zero R
	// primeiro tenta assert direto
	if typed, ok := req.(R); ok {
		return typed, nil
	}
	// tenta se for ponteiro para R (ex: *customer.Request quando R é customer.Request)
	rv := reflect.ValueOf(req)
	if rv.IsValid() && rv.Kind() == reflect.Ptr && !rv.IsNil() {
		elem := rv.Elem()
		if elem.IsValid() && elem.CanInterface() {
			if val, ok := elem.Interface().(R); ok {
				return val, nil
			}
		}
	}
	// tenta caso R seja ponteiro e req seja valor
	rv = reflect.ValueOf(req)
	if rv.IsValid() && rv.Kind() != reflect.Ptr {
		if reflect.TypeOf(zero).Kind() == reflect.Ptr {
			// criar ponteiro para o valor recebido se tipos baterem por nome
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
	return a.Ch.OnUpdate(ctx, typedReq, metadata, properties)
}

func (a *channelAdapter[E, R]) OnDelete(ctx context.Context, req any, metadata emitter.EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error) {
	typedReq, err := a.convertReq(req)
	if err != nil {
		return nil, err
	}
	return a.Ch.OnDelete(ctx, typedReq, metadata, properties)
}

func (a *channelAdapter[E, R]) OnCreate(ctx context.Context, req any, metadata emitter.EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error) {
	typedReq, err := a.convertReq(req)
	if err != nil {
		return nil, err
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
	logrus.Infof("emitter data (via reflection): %v metadata: %+v", dto, metadata)

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
