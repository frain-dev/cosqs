package mongo

import (
	"context"
	"net/url"
	"strings"
	"time"

	pager "github.com/gobeam/mongo-go-pagination"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	SourceCollection = "sources"
)

type PubSubType string

const (
	SQSPubSub    = "sqs"
	GooglePubSub = "google"
)

type Source struct {
	ID              primitive.ObjectID `json:"_" bson:"_id"`
	UID             string             `json:"uid" bson:"uid"`
	AccessKeyID     string             `json:"access_key_id" bson:"access_key_id"`
	SecretAccessKey string             `json:"secret_access_key" bson:"secret_access_key"`
	DefaultRegion   string             `json:"default_region" bson:"default_region"`
	Type            PubSubType         `json:"type" bson:"type"`
	Workers         int                `json:"workers" bson:"workers"`
	QueueName       string             `json:"queue_name" bson:"queue_name"`
}

type SourceRepository interface {
	CreateSource(ctx context.Context, source *Source) error
	LoadSources(ctx context.Context) ([]Source, error)
}

type Client struct {
	DB         *mongo.Database
	SourceRepo SourceRepository
}

func New(dsn string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// create a new mongodb client
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(dsn))
	if err != nil {
		return nil, err
	}

	//ping the client
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	name := strings.TrimPrefix(u.Path, "/")
	conn := client.Database(name, nil)

	c := &Client{
		DB:         conn,
		SourceRepo: NewSourceRepository(conn),
	}

	return c, nil
}

type sourceRepo struct {
	client *mongo.Collection
}

func NewSourceRepository(db *mongo.Database) SourceRepository {
	return &sourceRepo{
		client: db.Collection(SourceCollection),
	}
}

func (s *sourceRepo) CreateSource(ctx context.Context, source *Source) error {
	source.ID = primitive.NewObjectID()

	_, err := s.client.InsertOne(ctx, source)
	return err
}

func (s *sourceRepo) LoadSources(ctx context.Context) ([]Source, error) {
	var sources []Source

	_, err := pager.New(s.client).Context(ctx).Limit(100).Page(1).Sort("created_at", -1).Filter(bson.M{}).Decode(&sources).Find()
	if err != nil {
		return sources, err
	}

	if sources == nil {
		sources = make([]Source, 0)
	}

	return sources, nil
}
