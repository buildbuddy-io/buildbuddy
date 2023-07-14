package auditlog

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	alpb "github.com/buildbuddy-io/buildbuddy/proto/auditlog"
)

var (
	auditLogsEnabled = flag.Bool("app.audit_logs_enabled", false, "Whether to log administrative events to an audit log. Requires OLAP database to be configured.")
)

type Logger struct {
	env environment.Env
	dbh interfaces.OLAPDBHandle

	// Map of FooState protos to their corresponding fields in ResourceState proto.
	payloadTypes map[protoreflect.MessageDescriptor]protoreflect.FieldDescriptor
}

func GroupAPIKeyResourceID(id, name string) *alpb.ResourceID {
	return &alpb.ResourceID{
		Type: alpb.ResourceType_GROUP_API_KEY,
		Id:   id,
		Name: name,
	}
}

func UserAPIKeyResourceID(id, name string) *alpb.ResourceID {
	return &alpb.ResourceID{
		Type: alpb.ResourceType_USER_API_KEY,
		Id:   id,
		Name: name,
	}
}

func GroupResourceID(id string) *alpb.ResourceID {
	return &alpb.ResourceID{
		Type: alpb.ResourceType_GROUP,
		Id:   id,
	}
}

func SecretResourceID(secretName string) *alpb.ResourceID {
	return &alpb.ResourceID{
		Type: alpb.ResourceType_SECRET,
		Id:   secretName,
	}
}

func InvocationResourceID(id string) *alpb.ResourceID {
	return &alpb.ResourceID{
		Type: alpb.ResourceType_INVOCATION,
		Id:   id,
	}
}

func Register(env environment.Env) error {
	if !*auditLogsEnabled {
		return nil
	}

	if env.GetOLAPDBHandle() == nil {
		return status.FailedPreconditionErrorf("audit logs require an OLAP database")
	}

	payloadTypes := make(map[protoreflect.MessageDescriptor]protoreflect.FieldDescriptor)
	pfs := (&alpb.Entry_ResourceRequest{}).ProtoReflect().Descriptor().Fields()
	for i := 0; i < pfs.Len(); i++ {
		pf := pfs.Get(i)
		payloadTypes[pf.Message()] = pf
	}
	l := &Logger{
		env:          env,
		dbh:          env.GetOLAPDBHandle(),
		payloadTypes: payloadTypes,
	}
	env.SetAuditLogger(l)
	return nil
}

// wrapRequestProto automatically finds and sets the correct child message of
// the ResourceState proto based on the type of the passed proto.
func (l *Logger) wrapRequestProto(payload proto.Message) (*alpb.Entry_ResourceRequest, error) {
	fd, ok := l.payloadTypes[payload.ProtoReflect().Descriptor()]
	if !ok {
		return nil, status.InvalidArgumentErrorf("invalid payload proto: %s", payload.ProtoReflect().Descriptor())
	}
	payloadWrapper := &alpb.Entry_ResourceRequest{}
	payloadWrapper.ProtoReflect().Set(fd, protoreflect.ValueOfMessage(payload.ProtoReflect()))
	return payloadWrapper, nil
}

func clearRequestContext(request proto.Message) proto.Message {
	fd := request.ProtoReflect().Descriptor().Fields().ByName("request_context")
	if fd == nil {
		return request
	}
	request = proto.Clone(request)
	request.ProtoReflect().Clear(fd)
	return request
}

func (l *Logger) insertLog(ctx context.Context, resource *alpb.ResourceID, action alpb.Action, request proto.Message) error {
	u, err := l.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return status.WrapError(err, "auth failed")
	}
	if u.GetUserID() == "" {
		return nil
	}

	ui, err := l.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return status.WrapError(err, "could not lookup user")
	}

	request = clearRequestContext(request)

	var requestBytes []byte
	if request != nil {
		rp, err := l.wrapRequestProto(request)
		if err != nil {
			return status.WrapErrorf(err, "could not wrap request proto")
		}
		rpb, err := proto.Marshal(rp)
		if err != nil {
			return status.WrapErrorf(err, "could not marshal request proto")
		}
		requestBytes = rpb
	}

	entry := &schema.AuditLog{
		AuditLogID:    fmt.Sprintf("AL%d", random.RandUint64()),
		GroupID:       u.GetGroupID(),
		EventTimeUsec: time.Now().UnixMicro(),
		ClientIP:      clientip.Get(ctx),
		AuthUserID:    u.GetUserID(),
		AuthUserEmail: ui.Email,
		Action:        uint8(action),
		Request:       string(requestBytes),
	}
	if resource.GetType() != alpb.ResourceType_GROUP {
		entry.ResourceType = uint8(resource.Type)
		entry.ResourceID = resource.Id
		entry.ResourceName = resource.Name
	}

	if err := l.dbh.InsertAuditLog(ctx, entry); err != nil {
		return status.WrapError(err, "could not insert audit log")
	}

	return nil
}

// TODO(vadim): support API key auth
// TODO(vadim): populate client IP
func (l *Logger) Log(ctx context.Context, resource *alpb.ResourceID, action alpb.Action, request proto.Message) {
	if err := l.insertLog(ctx, resource, action, request); err != nil {
		log.Warningf("could not insert audit log: %s", err)
	}
}

func (l *Logger) GetLogs(ctx context.Context, req *alpb.GetAuditLogsRequest) (*alpb.GetAuditLogsResponse, error) {
	u, err := l.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}

	if err := authutil.AuthorizeGroupRole(u, u.GetGroupID(), role.Admin); err != nil {
		return nil, err
	}

	qb := query_builder.NewQuery(`
		SELECT * FROM AuditLogs
	`)
	qb.AddWhereClause("group_id = ?", u.GetGroupID())
	if req.GetTimestampBefore() != nil {
		qb.AddWhereClause("event_time_usec >= ?", req.GetTimestampAfter().AsTime().UnixMicro())
	}
	if req.GetTimestampBefore() != nil {
		qb.AddWhereClause("event_time_usec <= ?", req.GetTimestampBefore().AsTime().UnixMicro())
	}
	q, args := qb.Build()

	rows, err := l.dbh.DB(ctx).Raw(q, args).Rows()
	if err != nil {
		return nil, err
	}

	resp := &alpb.GetAuditLogsResponse{}
	for rows.Next() {
		var e schema.AuditLog
		if err := l.env.GetOLAPDBHandle().DB(ctx).ScanRows(rows, &e); err != nil {
			return nil, err
		}

		var request alpb.Entry_ResourceRequest
		if err := proto.Unmarshal([]byte(e.Request), &request); err != nil {
			return nil, err
		}

		resp.Entries = append(resp.Entries, &alpb.Entry{
			EventTime: timestamppb.New(time.UnixMicro(e.EventTimeUsec)),
			AuthenticationInfo: &alpb.AuthenticationInfo{
				User: &alpb.AuthenticatedUser{
					UserId:    e.AuthUserID,
					UserEmail: e.AuthUserEmail,
				},
			},
			Resource: &alpb.ResourceID{
				Type: alpb.ResourceType(e.ResourceType),
				Id:   e.ResourceID,
				Name: e.ResourceName,
			},
			Action:  alpb.Action(e.Action),
			Request: &request,
		})
	}

	return resp, nil
}
