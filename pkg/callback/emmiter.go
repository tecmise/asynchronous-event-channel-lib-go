package callback

import (
	"context"
	"errors"
	"github.com/sirupsen/logrus"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/emitter"
	"github.com/tecmise/connector-lib/pkg/ports/output/assync"
	"gorm.io/gorm"
	"reflect"
)

type AnyChannel interface {
	OnDelete(ctx context.Context, req any, emit emitter.Emitable[any]) (*assync.SnsTriggerResponse, error)
	OnCreate(ctx context.Context, req any, emit emitter.Emitable[any]) (*assync.SnsTriggerResponse, error)
	OnUpdate(ctx context.Context, req any, emit emitter.Emitable[any]) (*assync.SnsTriggerResponse, error)
}

type channelAdapter[T any] struct {
	ch            emitter.Channel[T]
	reflectedType string
}

func WrapperChannel[T any](ch emitter.Channel[T]) AnyChannel {
	return &channelAdapter[T]{
		ch:            ch,
		reflectedType: reflect.TypeOf(new(T)).Elem().String(),
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
	r, ok := req.(T)
	if !ok {
		return nil, errors.New("request type mismatch")
	}
	return a.ch.OnDelete(ctx, r, typedEmit)
}

func (a *channelAdapter[T]) OnCreate(ctx context.Context, req any, emit emitter.Emitable[any]) (*assync.SnsTriggerResponse, error) {
	typedEmit, err := a.toTypedEmitable(emit)
	if err != nil {
		return nil, err
	}
	r, ok := req.(T)
	if !ok {
		return nil, errors.New("request type mismatch")
	}
	return a.ch.OnCreate(ctx, r, typedEmit)
}

func (a *channelAdapter[T]) OnUpdate(ctx context.Context, req any, emit emitter.Emitable[any]) (*assync.SnsTriggerResponse, error) {
	typedEmit, err := a.toTypedEmitable(emit)
	if err != nil {
		return nil, err
	}
	r, ok := req.(T)
	if !ok {
		return nil, errors.New("request type mismatch")
	}
	return a.ch.OnUpdate(ctx, r, typedEmit)
}

func NewAsyncChannel() AsyncChannel {
	return &asyncChannel{}
}

type AsyncChannel interface {
	AddChannel(channel emitter.Channel[any]) AsyncChannel
	RegistryEmit(db *gorm.DB) error
}

type asyncChannel struct {
	channels map[reflect.Type]emitter.Channel[any]
}

func (a asyncChannel) RegistryEmit(db *gorm.DB) error {
	errInsert := db.Callback().Create().After("gorm:create").Register("emit_create", func(db *gorm.DB) {
		err := emit(db, "INSERT")
		if err != nil {
			logrus.Errorf("error on create database: %v", err)
		}
	})

	if errInsert != nil {
		logrus.Error("error to registry callback to insert")
		return errInsert
	}

	errUpdate := db.Callback().Create().After("gorm:update").Register("emit_update", func(db *gorm.DB) {
		err := emit(db, "UPDATE")
		if err != nil {
			logrus.Errorf("error on update database: %v", err)
		}
	})

	if errUpdate != nil {
		logrus.Error("error to registry callback to update")
		return errUpdate
	}

	errDelete := db.Callback().Create().After("gorm:delete").Register("emit_delete", func(db *gorm.DB) {
		err := emit(db, "DELETE")
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

func (a asyncChannel) AddChannel(channel emitter.Channel[any]) AsyncChannel {
	logrus.Debug("adding channel")
	key := reflect.TypeOf(new(any)).Elem()
	a.channels[key] = channel
	return a
}

func emit(db *gorm.DB, operation string) error {
	obj := db.Statement.Dest
	emitable, ok := obj.(emitter.Emitable[any])
	if ok {
		metadata := emitable.Metadada()
		fields := map[string]interface{}{
			"table":     db.Statement.Table,
			"publisher": metadata.Publisher,
			"name":      metadata.Name,
		}
		logrus.WithFields(fields).Debug("Emitting entity")

		channel := emitable.Channel()
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
