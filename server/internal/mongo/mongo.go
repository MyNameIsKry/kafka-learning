package mongostore

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"server/internal/events"
)

// Order statuses shown in Compass and (later) the UI history table.
const (
	OrderReceived   = "received"
	OrderProcessing = "processing"
	OrderDone       = "done"
	OrderError      = "error"
)

// Services whose done steps complete a kafka order.
var flowServices = []string{"inventory", "email", "shipping"}

type OrderStep struct {
	Service    string    `bson:"service" json:"service"`
	Status     string    `bson:"status" json:"status"`
	Message    string    `bson:"message" json:"message"`
	DurationMs int64     `bson:"durationMs" json:"durationMs"`
	At         time.Time `bson:"at" json:"at"`
}

// OrderDoc is one row of the orders collection, visible in Compass.
type OrderDoc struct {
	ID        string    `bson:"_id" json:"id"`
	ProductID string    `bson:"productId" json:"productId"`
	Qty       int       `bson:"qty" json:"qty"`
	Mode      string    `bson:"mode" json:"mode"`
	Burst     bool      `bson:"burst" json:"burst"`
	Status    string    `bson:"status" json:"status"`
	Steps     []OrderStep `bson:"steps" json:"steps"`
	TotalMs   int64     `bson:"totalMs" json:"totalMs"`
	CreatedAt time.Time `bson:"createdAt" json:"createdAt"`
	UpdatedAt time.Time `bson:"updatedAt" json:"updatedAt"`
}

type stockDoc struct {
	ID    string `bson:"_id"`
	Stock int    `bson:"stock"`
}

// Client wraps the collections the demo needs.
type Client struct {
	orders *mongo.Collection
	stock  *mongo.Collection
}

// Connect dials MongoDB and fails fast when it is unreachable.
// Mongo is a required dependency: start it with `make kafka-up`.
func Connect(uri, dbName string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mc, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("không nối được MongoDB (%s): %w — chạy `make kafka-up` trước", uri, err)
	}
	if err := mc.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("MongoDB không trả lời (%s): %w — chạy `make kafka-up` trước", uri, err)
	}
	db := mc.Database(dbName)
	return &Client{orders: db.Collection("orders"), stock: db.Collection("stock")}, nil
}

func opCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 3*time.Second)
}

// EnsureStock seeds the three products once; existing rows are untouched.
func (c *Client) EnsureStock() error {
	for id, qty := range events.InitialStock {
		ctx, cancel := opCtx()
		_, err := c.stock.UpdateOne(ctx,
			bson.M{"_id": id},
			bson.M{"$setOnInsert": bson.M{"stock": qty}},
			options.Update().SetUpsert(true))
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// DecreaseStock atomically subtracts qty, shared by every inventory
// instance. It reports the remaining stock for the UI message.
func (c *Client) DecreaseStock(productID string, qty int) (int, error) {
	ctx, cancel := opCtx()
	defer cancel()
	var out stockDoc
	err := c.stock.FindOneAndUpdate(ctx,
		bson.M{"_id": productID, "stock": bson.M{"$gte": qty}},
		bson.M{"$inc": bson.M{"stock": -qty}},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&out)
	if err == mongo.ErrNoDocuments {
		left := c.readStock(productID)
		return left, fmt.Errorf("không đủ tồn kho %s (còn %d, cần %d)", productID, left, qty)
	}
	if err != nil {
		return 0, err
	}
	return out.Stock, nil
}

func (c *Client) readStock(productID string) int {
	ctx, cancel := opCtx()
	defer cancel()
	var out stockDoc
	if err := c.stock.FindOne(ctx, bson.M{"_id": productID}).Decode(&out); err != nil {
		return 0
	}
	return out.Stock
}

// Snapshot returns the current stock of every product.
func (c *Client) Snapshot() map[string]int {
	ctx, cancel := opCtx()
	defer cancel()
	out := map[string]int{}
	cur, err := c.stock.Find(ctx, bson.M{})
	if err != nil {
		return out
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var d stockDoc
		if err := cur.Decode(&d); err == nil {
			out[d.ID] = d.Stock
		}
	}
	return out
}

// ResetStock puts every product back to its initial quantity.
func (c *Client) ResetStock() error {
	if err := c.EnsureStock(); err != nil {
		return err
	}
	for id, qty := range events.InitialStock {
		ctx, cancel := opCtx()
		_, err := c.stock.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"stock": qty}})
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// InsertOrder records a fresh order as received.
func (c *Client) InsertOrder(o events.Order) error {
	ctx, cancel := opCtx()
	defer cancel()
	_, err := c.orders.InsertOne(ctx, OrderDoc{
		ID: o.ID, ProductID: o.ProductID, Qty: o.Qty, Mode: o.Mode, Burst: o.Burst,
		Status: OrderReceived, Steps: []OrderStep{},
		CreatedAt: o.CreatedAt, UpdatedAt: time.Now(),
	})
	return err
}

// FinishOrder stamps the final latency and outcome of a sync order.
func (c *Client) FinishOrder(orderID string, totalMs int64, ok bool) {
	status := OrderDone
	if !ok {
		status = OrderError
	}
	ctx, cancel := opCtx()
	defer cancel()
	_, _ = c.orders.UpdateOne(ctx, bson.M{"_id": orderID}, bson.M{
		"$set": bson.M{"status": status, "totalMs": totalMs, "updatedAt": time.Now()},
	})
}

// ClearOrders wipes the history for a fresh demo run.
func (c *Client) ClearOrders() error {
	ctx, cancel := opCtx()
	defer cancel()
	_, err := c.orders.DeleteMany(ctx, bson.M{})
	return err
}

// OrderFilter mirrors the GET /api/orders query params. Empty means all.
type OrderFilter struct {
	Mode      string
	Status    string
	ProductID string
	Burst     string // "true" | "false" | "" (all)
	Page      int
	Limit     int
}

// OrderPage is the paginated envelope served to the UI history table.
type OrderPage struct {
	Orders []OrderDoc `bson:"orders" json:"orders"`
	Total  int64      `bson:"total" json:"total"`
	Page   int        `bson:"page" json:"page"`
	Pages  int        `bson:"pages" json:"pages"`
}

// ListOrderPage applies filters plus skip/limit and counts the total.
func (c *Client) ListOrderPage(f OrderFilter) OrderPage {
	if f.Limit <= 0 || f.Limit > 100 {
		f.Limit = 10
	}
	if f.Page <= 0 {
		f.Page = 1
	}
	filter := bson.M{}
	if f.Mode != "" {
		filter["mode"] = f.Mode
	}
	if f.Status != "" {
		filter["status"] = f.Status
	}
	if f.ProductID != "" {
		filter["productId"] = f.ProductID
	}
	if f.Burst == "true" {
		filter["burst"] = true
	} else if f.Burst == "false" {
		filter["burst"] = false
	}

	ctx, cancel := opCtx()
	defer cancel()
	total, _ := c.orders.CountDocuments(ctx, filter)
	pages := int((total + int64(f.Limit) - 1) / int64(f.Limit))

	out := OrderPage{Orders: []OrderDoc{}, Total: total, Page: f.Page, Pages: pages}
	cur, err := c.orders.Find(ctx, filter,
		options.Find().SetSort(bson.M{"createdAt": -1}).
			SetSkip(int64((f.Page - 1) * f.Limit)).
			SetLimit(int64(f.Limit)))
	if err != nil {
		return out
	}
	defer cur.Close(ctx)
	_ = cur.All(ctx, &out.Orders)
	if out.Orders == nil {
		out.Orders = []OrderDoc{}
	}
	return out
}

// ApplyLog folds one realtime StepLog into its order document.
// Unknown order IDs (hand-made probes) are ignored.
func (c *Client) ApplyLog(l events.StepLog) {
	ctx, cancel := opCtx()
	defer cancel()
	now := time.Now()

	switch {
	case l.Service == "gateway" && l.Status == events.StatusStart:
		_, _ = c.orders.UpdateOne(ctx,
			bson.M{"_id": l.OrderID, "status": OrderReceived},
			bson.M{"$set": bson.M{"status": OrderProcessing, "updatedAt": now}})

	case l.Service == "gateway" && l.Status == events.StatusError:
		_, _ = c.orders.UpdateOne(ctx, bson.M{"_id": l.OrderID}, bson.M{
			"$set": bson.M{"status": OrderError, "updatedAt": now}})

	case l.Service == "gateway":
		// Gateway "done" only ends a sync order; a published kafka order
		// is still being processed by the consumers.
		if l.Mode == events.ModeSync {
			_, _ = c.orders.UpdateOne(ctx, bson.M{"_id": l.OrderID}, bson.M{
				"$set": bson.M{"status": OrderDone, "updatedAt": now}})
		}
		c.upsertStep(ctx, l, now)

	default:
		c.upsertStep(ctx, l, now)
		if l.Status == events.StatusError {
			_, _ = c.orders.UpdateOne(ctx, bson.M{"_id": l.OrderID}, bson.M{
				"$set": bson.M{"status": OrderError, "updatedAt": now}})
			return
		}
		if l.Status == events.StatusDone {
			c.maybeComplete(ctx, l.OrderID, now)
		}
	}
}

func (c *Client) upsertStep(ctx context.Context, l events.StepLog, now time.Time) {
	step := OrderStep{Service: l.Service, Status: l.Status, Message: l.Message, DurationMs: l.DurationMs, At: now}
	res, _ := c.orders.UpdateOne(ctx,
		bson.M{"_id": l.OrderID, "steps.service": l.Service},
		bson.M{"$set": bson.M{
			"steps.$.status": l.Status, "steps.$.message": l.Message,
			"steps.$.durationMs": l.DurationMs, "steps.$.at": now, "updatedAt": now,
		}})
	if res != nil && res.ModifiedCount > 0 {
		return
	}
	_, _ = c.orders.UpdateOne(ctx, bson.M{"_id": l.OrderID}, bson.M{
		"$push": bson.M{"steps": step}, "$set": bson.M{"updatedAt": now}})
}

// maybeComplete flips a kafka order to done once all three flow
// services reported done. Missing docs (probes) simply match nothing.
func (c *Client) maybeComplete(ctx context.Context, orderID string, now time.Time) {
	var doc OrderDoc
	if err := c.orders.FindOne(ctx, bson.M{"_id": orderID}).Decode(&doc); err != nil {
		return
	}
	if doc.Mode != events.ModeKafka || doc.Status == OrderDone {
		return
	}
	done := map[string]bool{}
	for _, s := range doc.Steps {
		if s.Status == events.StatusDone {
			done[s.Service] = true
		}
	}
	for _, want := range flowServices {
		if !done[want] {
			return
		}
	}
	_, _ = c.orders.UpdateOne(ctx, bson.M{"_id": orderID}, bson.M{
		"$set": bson.M{
			"status": OrderDone, "updatedAt": now,
			"totalMs": now.Sub(doc.CreatedAt).Milliseconds(),
		}})
}
