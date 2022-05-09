package server

import (
	"github.com/tidwall/redcon"
	"sharedis/go/util"
	"time"
)

func (h *Handler) set(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_set"
	apiCounterVec.WithLabelValues(apiName, h.db).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, h.db).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	if len(cmd.Args) != 3 {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	encodeKey := EncodeKVKey(h.db, string(cmd.Args[1]))
	err := h.tdb.RawSet(encodeKey, cmd.Args[2], 0)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteNull()
		return
	}

	conn.WriteString("OK")
}

func (h *Handler) setex(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_setex"
	apiCounterVec.WithLabelValues(apiName, h.db).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, h.db).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	if len(cmd.Args) != 4 {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	ttl, err := util.BytesToInt32(cmd.Args[2])
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteNull()
		return
	}

	encodeKey := EncodeKVKey(h.db, string(cmd.Args[1]))
	err = h.tdb.RawSet(encodeKey, cmd.Args[3], ttl)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteNull()
		return
	}

	conn.WriteString("OK")
}

func (h *Handler) mset(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_multi_set"
	apiCounterVec.WithLabelValues(apiName, h.db).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, h.db).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	if len(cmd.Args) < 3 || len(cmd.Args) % 2 != 1 {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	keys := make([][]byte, 0)
	values := make([][]byte, 0)
	for idx := 1; idx < len(cmd.Args); {
		keys = append(keys, EncodeKVKey(h.db, string(cmd.Args[idx])))
		idx++
		values = append(values, cmd.Args[idx])
		idx++
	}

	err := h.tdb.RawMSet(keys, values, nil)
	if nil != err {
		conn.WriteNull()
		return
	}
	conn.WriteString("OK")
}

func (h *Handler) get(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_get"
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
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	encodeKey := EncodeKVKey(h.db, string(cmd.Args[1]))
	v, err := h.tdb.RawGet(encodeKey)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteNull()
		return
	}

	if nil == v {
		apiCounterNFVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteNull()
		return
	}

	conn.WriteBulk(v)
}

func (h *Handler) mget(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_mget"
	apiCounterVec.WithLabelValues(apiName, h.db).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, h.db).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	if len(cmd.Args) < 2 {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	keys := make([][]byte, len(cmd.Args) - 1)
	for i := 1; i < len(cmd.Args); i++ {
		keys[i - 1] = EncodeKVKey(h.db, string(cmd.Args[i]))
	}
	values, err := h.tdb.RawMGet(keys)
	if nil != err || nil == values {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteNull()
		return
	}

	conn.WriteArray(len(values))
	for _, v := range values {
		conn.WriteBulk(v)
	}
}
