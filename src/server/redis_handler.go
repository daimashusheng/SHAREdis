package server

import (
	"github.com/tidwall/redcon"
	"sharedis/sharedis"
	"time"
)

type Handler struct {
	db string
	tdb *sharedis.Sharedis
}

func NewHandler(tdb_v *sharedis.Sharedis) *Handler {
	return &Handler{ db : "0", tdb : tdb_v }
}

func NewServeMux(tdb *sharedis.Sharedis) *redcon.ServeMux {
	handler := NewHandler(tdb)
	mux := redcon.NewServeMux()
	mux.HandleFunc("ping", handler.ping)
	mux.HandleFunc("quit", handler.quit)
	mux.HandleFunc("del", handler.delete)
	mux.HandleFunc("set", handler.set)
	mux.HandleFunc("mset", handler.mset)
	mux.HandleFunc("setex", handler.setex)
	mux.HandleFunc("get", handler.get)
	mux.HandleFunc("mget", handler.mget)
	mux.HandleFunc("sadd", handler.sadd)
	mux.HandleFunc("srem", handler.srem)
	mux.HandleFunc("smembers", handler.smembers)
	mux.HandleFunc("scard", handler.scard)
	mux.HandleFunc("sismember", handler.sismember)
	mux.HandleFunc("hset", handler.hset)
	mux.HandleFunc("hmset", handler.hmset)
	mux.HandleFunc("hdel", handler.hdel)
	mux.HandleFunc("hget", handler.hget)
	mux.HandleFunc("hgetall", handler.hgetall)
	mux.HandleFunc("hlen", handler.hlen)
	return mux
}

func (h *Handler) ping(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("PONG")
}

func (h *Handler) quit(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("OK")
	conn.Close()
}

func (h *Handler) delete(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_del"
	apiCounterVec.WithLabelValues(apiName, h.db).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, h.db).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	encodeKey := EncodeKVKey(h.db, string(cmd.Args[1]))
	err := h.tdb.RawDel(encodeKey)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteInt(0)
		return
	}

	conn.WriteInt(1)
}
