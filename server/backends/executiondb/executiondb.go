package executiondb

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/golang/protobuf/proto"
	"github.com/jinzhu/gorm"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"google.golang.org/genproto/googleapis/longrunning"
)

type ExecutionDB struct {
	h *db.DBHandle
}

func NewExecutionDB(h *db.DBHandle) *ExecutionDB {
	return &ExecutionDB{h: h}
}

func (d *ExecutionDB) InsertOrUpdateExecution(ctx context.Context, executionID string, stage repb.ExecutionStage_Value, op *longrunning.Operation) error {
	execution := &tables.Execution{
		ExecutionID: executionID,
		Stage:       int64(stage),
	}
	if op != nil {
		opData, err := proto.Marshal(op)
		if err != nil {
			return err
		}
		execution.SerializedOperation = opData
	}

	return d.h.Transaction(func(tx *gorm.DB) error {
		var existing tables.Execution
		if err := tx.Where("execution_id = ?", executionID).First(&existing).Error; err != nil {
			if gorm.IsRecordNotFoundError(err) {
				return tx.Create(execution).Error
			}
			return err
		}
		return tx.Model(&existing).Where("execution_id = ?", executionID).Updates(execution).Error
	})
}

func (d *ExecutionDB) ReadExecution(ctx context.Context, executionID string) (*tables.Execution, error) {
	te := &tables.Execution{}
	existingRow := d.h.Raw(`SELECT * FROM Executions as e
                                WHERE e.execution_id = ?`, executionID)
	if err := existingRow.Scan(te).Error; err != nil {
		return nil, err
	}
	return te, nil
}
