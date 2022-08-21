package eventstorerepository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lamhai1401/even-driven-ex/evenstream/eventstore"
)

type repository struct {
	db *sql.DB
}

// New returns a concrete repository backed by CockroachDB
func New(db *sql.DB) (eventstore.Repository, error) {
	// return  repository
	return &repository{
		db: db,
	}, nil
}

func (repo *repository) CreateEvent(ctx context.Context, event *eventstore.Event) error {
	// Insert two rows into the "events" table.
	var err error
	var sql string
	if event.EventData != "" {
		sql = `INSERT INTO events (id, eventtype, aggregateid, aggregatetype, eventdata, stream) VALUES ($1, $2, $3, $4, $5, $6)`
		_, err = repo.db.ExecContext(ctx, sql, event.EventId, event.EventType, event.AggregateId, event.AggregateType, event.EventData, event.Stream)
	} else {
		sql = `INSERT INTO events (id, eventtype, aggregateid, aggregatetype, eventdata, stream) VALUES ($1, $2, $3, $4, NULL, $5)`
		_, err = repo.db.ExecContext(ctx, sql, event.EventId, event.EventType, event.AggregateId, event.AggregateType, event.Stream)
	}
	if err != nil {
		return fmt.Errorf("error on insert into events: %w", err)
	}
	return nil
}

// GetEvents query the events from event store by given filter
func (repo *repository) GetEvents(ctx context.Context, filter *eventstore.GetEventsRequest) ([]*eventstore.Event, error) {
	var rows *sql.Rows
	var err error
	var sql string
	if filter.EventId == "" && filter.AggregateId == "" {
		sql = `SELECT id, eventtype, aggregateid, aggregatetype, eventdata
               FROM events`
		rows, err = repo.db.QueryContext(ctx, sql)
	} else if filter.EventId != "" && filter.AggregateId == "" {
		sql = `SELECT id, eventtype, aggregateid, aggregatetype, eventdata
               FROM events WHERE id=$1`
		rows, err = repo.db.QueryContext(ctx, sql, filter.EventId)
	} else if filter.EventId == "" && filter.AggregateId != "" {
		sql = `SELECT id, eventtype, aggregateid, aggregatetype, eventdata
               FROM events WHERE aggregateid=$1`
		rows, err = repo.db.QueryContext(ctx, sql, filter.AggregateId)
	} else {
		sql = `SELECT id, eventtype, aggregateid, aggregatetype, eventdata
               FROM events WHERE id=$1 AND aggregateid=$2`
		rows, err = repo.db.QueryContext(ctx, sql, filter.EventId, filter.AggregateId)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []*eventstore.Event
	// Loop through rows, using Scan to assign column data to struct fields.
	for rows.Next() {
		var event eventstore.Event
		if err := rows.Scan(&event.EventId, &event.EventType,
			&event.AggregateId, &event.AggregateType, &event.EventData); err != nil {
			return events, err
		}
		events = append(events, &event)
	}

	if err = rows.Err(); err != nil {
		return events, err
	}

	return events, nil
}
