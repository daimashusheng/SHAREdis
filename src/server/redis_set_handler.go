package server

import (
	"github.com/pingcap/log"
	"github.com/tidwall/redcon"
	"go.uber.org/zap"
	"time"
)

func (h *Handler) sadd(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_ds_set"
	apiCounterVec.WithLabelValues(apiName, h.db).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, h.db).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	if len(cmd.Args) < 3 {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	keys := make([][]byte, len(cmd.Args) - 2)
	values := make([][]byte, len(cmd.Args) - 2)
	for idx, dm := range cmd.Args {
		if idx < 2 {
			continue
		}
		keys[idx - 2] = EncodeDedupSetKey(h.db, string(cmd.Args[1]), string(dm))
		// tikv doesn't support empty value, so we must set a space
		// although dset doesn't need value
		values[idx - 2] = []byte(" ")
	}

	err := h.tdb.RawMSet(keys, values, nil)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		log.Error("DS set error",
			zap.String("segment", h.db),
			zap.String("key", string(cmd.Args[1])),
			zap.Error(err))
		conn.WriteNull()
		return
	}
	conn.WriteInt(len(cmd.Args) - 2)
}

func (h *Handler) srem(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_ds_rem"
	apiCounterVec.WithLabelValues(apiName, h.db).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd-tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, h.db).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	if len(cmd.Args) < 3 {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	delKeys := make([][]byte, len(cmd.Args) - 2)
	for idx, dm := range cmd.Args {
		if idx < 2 {
			continue
		}
		delKeys[idx - 2] = EncodeDedupSetKey(h.db, string(cmd.Args[1]), string(dm))
	}

	err := h.tdb.RawMDel(delKeys)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		log.Error("DS rem error",
			zap.String("segment", h.db),
			zap.String("key", string(cmd.Args[1])),
			zap.Error(err))
		conn.WriteNull()
		return
	}
	conn.WriteInt(len(cmd.Args) - 2)
}

func (h *Handler) smembers(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_ds_get"
	apiCounterVec.WithLabelValues(apiName, h.db).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd-tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, h.db).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	if len(cmd.Args) != 2 {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	keyPre := EncodeDedupSetKeyPrefix(h.db, string(cmd.Args[1]))
	keys, _, err := h.tdb.GetKeysByPrefix(keyPre, 0)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		log.Error("DS get error",
			zap.String("segment", h.db),
			zap.String("key", string(cmd.Args[1])),
			zap.Error(err))
		conn.WriteNull()
		return
	}
	if nil == keys {
		conn.WriteArray(0)
		return
	}
	members := make([]string, 0)
	for _, encodeKey := range keys {
		ret, key, member := DecodeDedupSetKey(encodeKey)
		if !ret {
			continue
		}
		if key != string(cmd.Args[1]) {
			log.Error("decoded key is error",
				zap.String("okey", string(cmd.Args[1])),
				zap.String("dkey", string(encodeKey)))
			continue
		}
		members = append(members, member)
	}
	conn.WriteArray(len(members))
	for _, v := range members {
		conn.WriteString(v)
	}
}

func (h *Handler) scard(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_ds_count"
	apiCounterVec.WithLabelValues(apiName, h.db).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd-tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, h.db).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	if len(cmd.Args) != 2 {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	keyPre := EncodeDedupSetKeyPrefix(h.db, string(cmd.Args[1]))
	keys, _, err := h.tdb.GetKeysByPrefix(keyPre, 0)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		log.Error("DS count error",
			zap.String("segment", h.db),
			zap.String("key", string(cmd.Args[1])),
			zap.Error(err))
		conn.WriteNull()
		return
	}
	conn.WriteInt(len(keys))
}

func (h *Handler) sismember(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_ds_is_member"
	apiCounterVec.WithLabelValues(apiName, h.db).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd-tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, h.db).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	if len(cmd.Args) != 3 {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	encodeKey := EncodeDedupSetKey(h.db, string(cmd.Args[1]), string(cmd.Args[2]))
	v, err := h.tdb.RawGet(encodeKey)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		log.Error("DS ismember error",
			zap.String("segment", string(cmd.Args[1])),
			zap.String("key", string(cmd.Args[2])),
			zap.Error(err))
		conn.WriteNull()
		return
	}
	if nil == v {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}
