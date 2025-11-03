package callback

import (
	"context"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/emitter"
	"github.com/tecmise/connector-lib/pkg/ports/output/assync"
	"gorm.io/gorm"
	"reflect"
	"strings"
)

type AnyChannel interface {
	OnDelete(ctx context.Context, req any, emit emitter.Emitable[any]) (*assync.SnsTriggerResponse, error)
	OnCreate(ctx context.Context, req any, emit emitter.Emitable[any]) (*assync.SnsTriggerResponse, error)
	OnUpdate(ctx context.Context, req any, emit emitter.Emitable[any]) (*assync.SnsTriggerResponse, error)
}

type channelAdapter[T any] struct {
	Ch            emitter.Channel[T]
	ReflectedType reflect.Type
}

func WrapperChannel[T any](ch emitter.Channel[T]) AnyChannel {
	return &channelAdapter[T]{
		Ch:            ch,
		ReflectedType: reflect.TypeOf(new(T)).Elem(),
	}
}

func (a *channelAdapter[T]) toTypedEmitable(em emitter.Emitable[any]) (emitter.Emitable[T], error) {
	if typed, ok := em.(emitter.Emitable[T]); ok {
		return typed, nil
	}
	return nil, errors.New("emitable type mismatch")
}

func (a *channelAdapter[T]) OnDelete(ctx context.Context, req any, emit emitter.Emitable[any]) (*assync.SnsTriggerResponse, error) {
	typedEmit, err := a.toTypedEmitable(emit)
	if err != nil {
		return nil, err
	}
	r, ok := req.(*T)
	if !ok {
		return nil, errors.New("request type mismatch")
	}
	return a.Ch.OnDelete(ctx, *r, typedEmit)
}

func (a *channelAdapter[T]) OnCreate(ctx context.Context, req any, emit emitter.Emitable[any]) (*assync.SnsTriggerResponse, error) {
	typedEmit, err := a.toTypedEmitable(emit)
	if err != nil {
		return nil, err
	}
	r, ok := req.(*T)
	if !ok {
		return nil, errors.New("request type mismatch")
	}
	return a.Ch.OnCreate(ctx, *r, typedEmit)
}

func (a *channelAdapter[T]) OnUpdate(ctx context.Context, req any, emit emitter.Emitable[any]) (*assync.SnsTriggerResponse, error) {
	typedEmit, err := a.toTypedEmitable(emit)
	if err != nil {
		return nil, err
	}
	r, ok := req.(*T)
	if !ok {
		return nil, errors.New("request type mismatch")
	}
	return a.Ch.OnUpdate(ctx, *r, typedEmit)
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

		field := rv.FieldByName("ReflectedType")
		if !field.IsValid() {
			logrus.Error("field ReflectedType not found on channel")
			return
		}

		typ, ok := field.Interface().(reflect.Type)
		if !ok {
			logrus.Error("ReflectedType has unexpected type")
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
	emitable, ok := obj.(emitter.Emitable[any])
	if ok {
		metadata := emitable.Metadada()
		key := reflect.TypeOf(obj).String()

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

		var err error
		var result *assync.SnsTriggerResponse
		if operation == "DELETE" {
			logrus.WithFields(fields).Debug("Deleting entity")
			result, err = channel.OnDelete(db.Statement.Context, obj, emitable)
		} else if operation == "INSERT" {
			logrus.WithFields(fields).Debug("Inserting entity")
			result, err = channel.OnCreate(db.Statement.Context, obj, emitable)
		} else if operation == "UPDATE" {
			logrus.WithFields(fields).Debug("Updating entity")
			result, err = channel.OnUpdate(db.Statement.Context, obj, emitable)
		}
		if err != nil {
			logrus.Errorf("error emitting entity: %v", err)
			return err
		}
		if result == nil {
			return errors.New("error emitting entity: no result")
		}
		logrus.WithField("message_id", result.MessageId).Debug("emited entity")
	}
	return nil
}
