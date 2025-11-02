package callback

import (
	"errors"
	"github.com/sirupsen/logrus"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/emitter"
	"github.com/tecmise/connector-lib/pkg/ports/output/assync"
	"gorm.io/gorm"
)

func EmitDeletion(db *gorm.DB) error {
	return emit(db, "DELETE")
}

func EmitInsertion(db *gorm.DB) error {
	return emit(db, "INSERT")
}

func EmitUpdate(db *gorm.DB) error {
	return emit(db, "UPDATE")
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
