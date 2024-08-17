package notifications

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"

	nfpb "github.com/buildbuddy-io/buildbuddy/proto/notification"
)

var (
	enabled       = flag.Bool("notifications.enabled", false, "Whether to enable notifications.")
	pollInterval  = flag.Duration("notifications.poll_interval", 1*time.Minute, "How often to query the Notifications table for undelivered notifications. Polling is used as a backup in case Redis becomes unavailable.")
	workerCount   = flag.Int("notifications.delivery_workers", 2, "Number of notification workers to run concurrently. Each worker reads from the notifications table and delivers payloads to the appropriate backend.")
	subjectPrefix = flag.String("notifications.email.subject_prefix", "[BuildBuddy] ", "Prefix added to the start of all email subject lines.")
	fromName      = flag.String("notifications.email.from_name", "BuildBuddy", "Sender name for automated email notifications.")
	fromAddress   = flag.String("notifications.email.from_address", "", "Sender address for automated email notifications.")
)

const (
	// Blobstore directory where all alert payloads are stored.
	blobstorePrefix = "notifications/payloads/"

	// Redis key containing a queue of notification IDs.
	redisNotificationIDsKey = "notifications/committed_notification_ids"

	// Redis key where apps can proactively subscribe to updates to the
	// notification ID queue.
	redisPubSubKey = "notifications/pubsub"

	// Default notification expiry. If it is not delivered in this long, then it
	// will not be delivered at all.
	defaultExpiry = 24 * time.Hour

	// Enriched logging context key for notification ID.
	notificationIDLogContextKey = "notification_id"
)

func Enabled() bool {
	return *enabled
}

// TX is a notification transaction.
// The only allowed implementation is *transaction, which can be created
// using the Transaction func.
type TX interface {
	interfaces.DB
	internal()
}

type transaction struct {
	interfaces.DB
	// Callbacks invoked after the transaction is successfully committed.
	// These are used to trigger best-effort signals to apps that there are
	// notifications ready to be delivered.
	onCommit []func(ctx context.Context) error
}

func (t *transaction) internal() {}

// Transaction starts a new DB transaction that may result in a notification
// being delivered.
func Transaction(ctx context.Context, dbh interfaces.DBHandle, fn func(tx TX) error) error {
	txInternal := &transaction{}
	err := dbh.Transaction(ctx, func(tx interfaces.DB) error {
		txInternal.DB = tx
		return fn(txInternal)
	})
	// If the transaction succeeded, invoke callbacks.
	if err != nil {
		return err
	}
	for _, cb := range txInternal.onCommit {
		if err := cb(ctx); err != nil {
			log.CtxWarningf(ctx, "Notification post-insert hook failed: %s", err)
		}
	}
	return nil
}

type service struct {
	blobstore interfaces.Blobstore
	rdb       redis.UniversalClient
}

func NewService(blobstore interfaces.Blobstore, rdb redis.UniversalClient) (*service, error) {
	return &service{
		blobstore: blobstore,
		rdb:       rdb,
	}, nil
}

// TODO: expiresAt
func (s *service) Create(ctx context.Context, tx TX, groupID, contentID string, notification *nfpb.Notification) (created bool, err error) {
	if groupID == "" {
		return false, status.InvalidArgumentError("missing group ID")
	}
	if contentID == "" {
		return false, status.InvalidArgumentErrorf("missing content ID")
	}

	// Upload to blobstore under the blob ID.
	b, err := proto.Marshal(notification)
	if err != nil {
		return false, status.InternalErrorf("marshal notification: %s", err)
	}
	blobName := blobstorePrefix + fmt.Sprintf("sha256/%x", sha256.Sum256(b))
	// TODO: handle "AlreadyExists"?
	if _, err := s.blobstore.WriteBlob(ctx, blobName, b); err != nil {
		return false, status.InternalErrorf("write notification to blobstore: %s", err)
	}

	pk, err := tables.PrimaryKeyForTable((*tables.Notification)(nil).TableName())
	if err != nil {
		return false, status.InternalErrorf("generate primary key: %s", err)
	}
	ctx = log.EnrichContext(ctx, notificationIDLogContextKey, pk)

	now := time.Now()
	err = tx.NewQuery(ctx, "notifications_insert").Create(&tables.Notification{
		NotificationID: pk,
		IdempotencyKey: hash.Strings(groupID, contentID),
		// TODO: have a separate NotificationDeliveries table, one per delivery
		// format (SMS, email, webhook, etc.), and have separate Delivered bits
		// for each.
		Delivered:       false,
		GroupID:         groupID,
		TriggeredAtUsec: now.UnixMicro(),
		ExpiresAtUsec:   now.Add(defaultExpiry).UnixMicro(),
		PayloadBlobName: blobName,
	})

	// TODO: return (false, nil) if row already exists
	if err != nil {
		return false, err
	}

	// When the transaction is committed, publish a message so that apps know
	// the notification is ready to be delivered.
	txInternal := tx.(*transaction)
	txInternal.onCommit = append(txInternal.onCommit, func(ctx context.Context) error {
		ctx = log.EnrichContext(ctx, notificationIDLogContextKey, pk)
		log.CtxDebugf(ctx, "Notification transaction committed; sending redis pubsub message")
		if err := redisutil.UnreliableWorkQueueEnqueue(ctx, s.rdb, redisNotificationIDsKey, pk, float64(time.Now().UnixMicro())); err != nil {
			log.CtxErrorf(ctx, "Failed to enqueue notification ID to redis queue (notification will be delayed until next DB poll): %s", err)
		}
		return err
	})

	return true, nil
}

type EmailClient interface {
	SendEmail(ctx context.Context, idempotencyKey, fromName, fromAddress string, toAddresses []string, subject, contentType, content string) error
}

type deliveryWorkerPool struct {
	dbh         interfaces.DBHandle
	userdb      interfaces.UserDB
	rdb         redis.UniversalClient
	blobstore   interfaces.Blobstore
	emailClient EmailClient

	q        *redisutil.UnreliableWorkQueue
	shutdown chan struct{}
	done     chan struct{}
}

func NewDeliveryWorkerPool(ctx context.Context, dbh interfaces.DBHandle, userdb interfaces.UserDB, rdb redis.UniversalClient, blobstore interfaces.Blobstore, emailClient EmailClient) (*deliveryWorkerPool, error) {
	q, err := redisutil.NewUnreliableWorkQueue(ctx, rdb, redisNotificationIDsKey)
	if err != nil {
		return nil, status.WrapError(err, "initialize work queue")
	}
	return &deliveryWorkerPool{
		dbh:         dbh,
		rdb:         rdb,
		blobstore:   blobstore,
		userdb:      userdb,
		emailClient: emailClient,
		q:           q,
		shutdown:    make(chan struct{}),
		done:        make(chan struct{}),
	}, nil
}

// Start starts a pool of workers to deliver notifications to the configured
// backends.
//
// The DB is polled periodically, and a Redis stream is also used to allow
// proactively request individual notification IDs to be delivered.
func (p *deliveryWorkerPool) Start(ctx context.Context) {
	go p.run(ctx)
}

func (p *deliveryWorkerPool) run(ctx context.Context) {
	defer close(p.done)

	shutdownCtx, cancelShutdownCtx := context.WithCancel(ctx)
	go func() {
		<-p.shutdown
		cancelShutdownCtx()
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	ch := make(chan string)
	defer close(ch)

	for range *workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for notificationID := range ch {
				ctx := ctx
				if notificationID != "" {
					ctx = log.EnrichContext(ctx, notificationIDLogContextKey, notificationID)
					log.CtxDebugf(ctx, "Attempting notification delivery")
				} else {
					log.CtxDebugf(ctx, "Polling for undelivered notifications")
				}
				if err := p.attemptDelivery(ctx, notificationID); err != nil {
					log.CtxWarningf(ctx, "Delivery attempt failed: %s", err)
				}
				// TODO: ignore "DB Locked" errors?
			}
		}()
	}

	for {
		notificationID := p.getNotificationID(shutdownCtx)
		if shutdownCtx.Err() != nil {
			return
		}
		ch <- notificationID
	}
}

func (p *deliveryWorkerPool) getNotificationID(ctx context.Context) string {
	timeoutCtx, cancel := context.WithTimeout(ctx, *pollInterval)
	defer cancel()
	notificationID, err := p.q.Dequeue(timeoutCtx)
	if err != nil && ctx.Err() != nil {
		log.CtxErrorf(ctx, "Failed to dequeue notification ID: %s", err)
	}
	// Returning an empty notification ID here (if err != nil)
	// means "get any undelivered notification"
	return notificationID
}

func (p *deliveryWorkerPool) attemptDelivery(ctx context.Context, notificationID string) error {
	return p.dbh.Transaction(ctx, func(tx interfaces.DB) error {
		// If we have a notification ID, query it from the DB directly.
		// Otherwise select the oldest notification that's still
		// undelivered.
		qb := query_builder.NewQuery(`SELECT * FROM "Notifications"`)
		qb.AddWhereClause(`delivered = 0`)
		qb.AddWhereClause(`expires_at_usec = 0 OR expires_at_usec > ?`, time.Now().UnixMicro())
		if notificationID != "" {
			qb.AddWhereClause(`notification_id = ?`, notificationID)
		}
		// Prioritize older notifications
		qb.SetOrderBy(`triggered_at_usec`, true /*asc*/)
		qb.SetLimit(1)
		q, args := qb.Build()
		q += ` ` + p.dbh.SelectForUpdateModifier()
		notification := &tables.Notification{}
		err := tx.NewQuery(ctx, "notifications_find_undelivered_notification").Raw(q, args...).Take(notification)
		if db.IsRecordNotFound(err) {
			log.CtxDebugf(ctx, "No undelivered notifications found")
			return nil
		}
		if err != nil {
			return status.InternalErrorf("find undelivered notification: %s", err)
		}
		ctx = log.EnrichContext(ctx, notificationIDLogContextKey, notification.NotificationID)

		// Get email recipients
		groupAdminEmails, err := p.userdb.GetGroupAdminUserEmailsForAlerts(ctx, tx, notification.GroupID)
		if err != nil {
			return status.WrapError(err, "get admin user emails")
		}

		// Look up the email payload from blobstore
		b, err := p.blobstore.ReadBlob(ctx, notification.PayloadBlobName)
		if err != nil {
			// TODO: if NotExists, delete the Notification so we don't keep
			// re-attempting.
			return status.WrapErrorf(err, "read notification payload (blob %q)", notification.PayloadBlobName)
		}
		payload := &nfpb.Notification{}
		if err := proto.Unmarshal(b, payload); err != nil {
			return status.InternalErrorf("malformed blob: %s", err)
		}

		// Deliver Email if applicable
		// TODO: validate payload
		if payload.Email != nil {
			if err := p.emailClient.SendEmail(ctx, notification.IdempotencyKey, *fromName, *fromAddress, groupAdminEmails, *subjectPrefix+payload.GetEmail().GetSubject(), "text/html", payload.GetEmail().GetBody()); err != nil {
				return status.WrapError(err, "send email")
			}
		}

		// Delivery succeeded (if applicable).
		// Mark delivered.
		err = tx.NewQuery(ctx, "notifications_mark_delivered").Raw(`
			UPDATE "Notifications"
			SET delivered = 1
			WHERE notification_id = ?
		`, notification.NotificationID).Exec().Error
		if err != nil {
			return status.WrapErrorf(err, "mark notification %q delivered", notificationID)
		}
		return nil
	})
}

func (p *deliveryWorkerPool) Shutdown() {
	log.Debugf("Stopping notification delivery.")
	close(p.shutdown)
	<-p.done
	log.Debugf("Notification delivery stopped.")
}
