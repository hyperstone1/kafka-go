package repository

import (
	"context"
	"fmt"
	"os"
	"github.com/hyperstone1/go-kafka/model"
	"github.com/jackc/pgx/v4"
)

type Connection struct {
	conn *pgx.Conn
}

func New() *Connection {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres:1234@localhost:5432/msg")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	return &Connection{conn: conn}
}

func (c *Connection) Record(key, value string) (model.Mes, error) {
	var msg model.Mes
	var err error
	query := c.conn.QueryRow(context.Background(), "INSERT INTO msg (key, value) VALUES ($1, $2)", key, value)
	query.Scan(&msg.Key, &msg.Value)

	return msg, err
}
