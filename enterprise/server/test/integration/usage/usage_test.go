package usage_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
)

func TestGetUsage_ParentAdminFetchesChildGroupUsage(t *testing.T) {
	// Start enterprise app
	app := buildbuddy_enterprise.Run(
		t,
		"--app.usage_tracking_enabled=true",
		"--app.region=test",                // Required for usage tracking
		"--auth.api_key_group_cache_ttl=0", // Don't cache API key -> group mappings for this test
	)
	wc := buildbuddy_enterprise.LoginAsDefaultSelfAuthUser(t, app)

	// Treat the logged-in user's selected group as the parent org; mark it as parent and
	// set SAML metadata so that authdb recognizes child orgs sharing the same IdP.
	parentGroupID := wc.RequestContext.GetGroupId()
	parent := &tables.Group{}
	require.NoError(t, app.DB().Where("group_id = ?", parentGroupID).Take(parent).Error)
	parent.IsParent = true
	parent.SamlIdpMetadataUrl = "https://idp.example.test/metadata"
	parent.URLIdentifier = "parent-org"
	require.NoError(t, app.DB().Save(parent).Error)

	// Create an ORG_ADMIN API key for the parent org using cookie-authenticated Web RPC.
	createKeyReq := &akpb.CreateApiKeyRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: parentGroupID},
		Label:          "admin-1",
		Capability:     []cappb.Capability{cappb.Capability_ORG_ADMIN},
	}
	createKeyRsp := &akpb.CreateApiKeyResponse{}
	require.NoError(t, wc.RPC("CreateApiKey", createKeyReq, createKeyRsp))
	adminAPIKey := createKeyRsp.GetApiKey().GetValue()
	require.NotEmpty(t, adminAPIKey)

	// Create a child group (same SAML as parent) using the API.
	// Do NOT add the user as a member: authenticate with the parent ORG_ADMIN API key.
	createChildGrpReq, err := http.NewRequest(
		http.MethodPost,
		app.HTTPURL()+"/rpc/BuildBuddyService/CreateGroup",
		bytes.NewBufferString(`{"name":"child-org","urlIdentifier":"child-org"}`),
	)
	require.NoError(t, err)
	createChildGrpReq.Header.Set("Content-Type", "application/json")
	createChildGrpReq.Header.Set("x-buildbuddy-api-key", adminAPIKey)
	createChildGrpResp, err := http.DefaultClient.Do(createChildGrpReq)
	require.NoError(t, err)
	defer createChildGrpResp.Body.Close()
	createResBytes, err := io.ReadAll(createChildGrpResp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, createChildGrpResp.StatusCode)
	childGrpRsp := &grpb.CreateGroupResponse{}
	require.NoError(t, protojson.Unmarshal(createResBytes, childGrpRsp))
	childGroupID := childGrpRsp.GetId()
	require.NotEmpty(t, childGroupID)
	child := &tables.Group{}
	require.NoError(t, app.DB().Where("group_id = ?", childGroupID).Take(child).Error)

	// Seed a usage row for the child group.
	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	seeded := &tables.Usage{
		GroupID:         childGroupID,
		PeriodStartUsec: monthStart.UnixMicro(),
		Region:          "test",
		FinalBeforeUsec: monthStart.Add(30 * 24 * time.Hour).UnixMicro(),
		UsageCounts: tables.UsageCounts{
			Invocations:                3,
			CASCacheHits:               1,
			ActionCacheHits:            2,
			TotalDownloadSizeBytes:     2048,
			LinuxExecutionDurationUsec: 111,
			TotalUploadSizeBytes:       4096,
			TotalCachedActionExecUsec:  222,
			CPUNanos:                   333,
		},
		UsageLabels: tables.UsageLabels{Origin: "internal", Client: "bazel", Server: "app"},
	}
	db := app.DB()
	require.NoError(t, db.Create(seeded).Error)

	// Make the HTTP protolet request using the parent admin API key and requesting the child group.
	// Use JSON body for documentation clarity.
	httpReq, err := http.NewRequest(
		http.MethodPost,
		app.HTTPURL()+"/rpc/BuildBuddyService/GetUsage",
		bytes.NewBufferString(fmt.Sprintf(`{"requestContext":{"groupId":"%s"}}`, childGroupID)),
	)
	require.NoError(t, err)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-buildbuddy-api-key", adminAPIKey)
	httpRes, err := http.DefaultClient.Do(httpReq)
	require.NoError(t, err)
	defer httpRes.Body.Close()
	resBytes, err := io.ReadAll(httpRes.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, httpRes.StatusCode)
	rsp := &usagepb.GetUsageResponse{}
	require.NoError(t, protojson.Unmarshal(resBytes, rsp))

	require.NotNil(t, rsp.GetUsage())
	assert.EqualValues(t, 3, rsp.GetUsage().GetInvocations())
	assert.EqualValues(t, 2, rsp.GetUsage().GetActionCacheHits())
	assert.EqualValues(t, 1, rsp.GetUsage().GetCasCacheHits())
	assert.EqualValues(t, 2048, rsp.GetUsage().GetTotalDownloadSizeBytes())
	assert.EqualValues(t, 4096, rsp.GetUsage().GetTotalUploadSizeBytes())
	assert.EqualValues(t, 111, rsp.GetUsage().GetLinuxExecutionDurationUsec())
	assert.EqualValues(t, 222, rsp.GetUsage().GetTotalCachedActionExecUsec())
	assert.EqualValues(t, 333, rsp.GetUsage().GetCloudCpuNanos())
	require.Len(t, rsp.GetDailyUsage(), 1)
	assert.EqualValues(t, 3, rsp.GetDailyUsage()[0].GetInvocations())
}
