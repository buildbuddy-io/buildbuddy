package executiondb

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/jinzhu/gorm"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"google.golang.org/genproto/googleapis/longrunning"
)

type ExecutionDB struct {
	env environment.Env
	h   *db.DBHandle
}

func NewExecutionDB(env environment.Env, h *db.DBHandle) *ExecutionDB {
	return &ExecutionDB{
		env: env,
		h:   h,
	}
}

func (d *ExecutionDB) createExecution(tx *gorm.DB, ctx context.Context, summary *tables.ExecutionSummary) error {
	permissions := perms.AnonymousUserPermissions()
	if auth := d.env.GetAuthenticator(); auth != nil {
		if u, err := auth.AuthenticatedUser(ctx); err == nil && u.GetGroupID() != "" {
			permissions = perms.GroupAuthPermissions(u.GetGroupID())
		}
	}
	summary.UserID = permissions.UserID
	summary.GroupID = permissions.GroupID
	summary.Perms = permissions.Perms
	return tx.Create(summary).Error
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

func (d *ExecutionDB) InsertExecutionSummary(ctx context.Context, actionDigest *repb.Digest, workerID, invocationID string, summary *espb.ExecutionSummary) error {
	pk, err := tables.PrimaryKeyForTable("ExecutionSummaries")
	if err != nil {
		return err
	}
	tableSummary := &tables.ExecutionSummary{
		SummaryID:                  pk,
		InvocationID:               invocationID,
		ActionDigest:               fmt.Sprintf("%s/%d", actionDigest.GetHash(), actionDigest.GetSizeBytes()),
		WorkerID:                   workerID,
		UserCpuTimeUsec:            summary.GetExecutionStats().GetUserCpuTimeUsec(),
		SysCpuTimeUsec:             summary.GetExecutionStats().GetSysCpuTimeUsec(),
		MaxResidentSetSizeBytes:    summary.GetExecutionStats().GetMaxResidentSetSizeBytes(),
		PageReclaims:               summary.GetExecutionStats().GetPageReclaims(),
		PageFaults:                 summary.GetExecutionStats().GetPageFaults(),
		Swaps:                      summary.GetExecutionStats().GetSwaps(),
		BlockInputOperations:       summary.GetExecutionStats().GetBlockInputOperations(),
		BlockOutputOperations:      summary.GetExecutionStats().GetBlockOutputOperations(),
		MessagesSent:               summary.GetExecutionStats().GetMessagesSent(),
		MessagesReceived:           summary.GetExecutionStats().GetMessagesReceived(),
		SignalsReceived:            summary.GetExecutionStats().GetSignalsReceived(),
		VoluntaryContextSwitches:   summary.GetExecutionStats().GetVoluntaryContextSwitches(),
		InvoluntaryContextSwitches: summary.GetExecutionStats().GetInvoluntaryContextSwitches(),
		FileDownloadCount:          summary.GetIoStats().GetFileDownloadCount(),
		FileDownloadSizeBytes:      summary.GetIoStats().GetFileDownloadSizeBytes(),
		FileDownloadDurationUsec:   summary.GetIoStats().GetFileDownloadDurationUsec(),
		FileUploadCount:            summary.GetIoStats().GetFileUploadCount(),
		FileUploadSizeBytes:        summary.GetIoStats().GetFileUploadSizeBytes(),
		FileUploadDurationUsec:     summary.GetIoStats().GetFileUploadDurationUsec(),
	}
	return d.h.Transaction(func(tx *gorm.DB) error {
		var existing tables.ExecutionSummary
		if err := tx.Where("summary_id = ?", tableSummary.SummaryID).First(&existing).Error; err != nil {
			if gorm.IsRecordNotFoundError(err) {
				return d.createExecution(tx, ctx, tableSummary)
			}
			return err
		}
		return status.FailedPreconditionError("User already exists!")
	})
}
