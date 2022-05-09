package server

import (
	"github.com/pingcap/log"
	"github.com/tidwall/redcon"
	"go.uber.org/zap"
	"time"
)

func (h *Handler) hset(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_hash_set"
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

	key := EncodeHashKey(h.db, string(cmd.Args[1]), string(cmd.Args[2]))
	err := h.tdb.RawSet(key, cmd.Args[3], 0)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		log.Error("set field and value failed",
			zap.String("key", string(cmd.Args[1])),
			zap.Error(err))
		conn.WriteNull()
		return
	}

	conn.WriteInt(1)
}

func (h *Handler) hmset(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_multi_hash_set"
	apiCounterVec.WithLabelValues(apiName, h.db).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, h.db).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	if len(cmd.Args) < 4 && len(cmd.Args) % 2 != 0 {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	keys := make([][]byte, 0)
	values := make([][]byte, 0)
	ttls := make([]uint64, 0)
	for idx, dm := range cmd.Args {
		if idx < 2 {
			continue
		}
		if idx % 2 == 0 {
			keys = append(keys, EncodeHashKey(h.db, string(cmd.Args[1]), string(dm)))
		} else {
			values = append(values, dm)
			ttls = append(ttls, 0)
		}
	}

	err := h.tdb.RawMSet(keys, values, ttls)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		log.Error("set fields and values failed",
			zap.String("key", string(cmd.Args[1])),
			zap.Error(err))
		conn.WriteNull()
		return
	}

	conn.WriteString("OK")
}

func (h *Handler) hdel(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_hash_rem"
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

	del_keys := make([][]byte, 0)
	for idx, dm := range cmd.Args {
		if idx < 2 {
			continue
		}
		del_keys = append(del_keys, EncodeHashKey(h.db, string(cmd.Args[1]), string(dm)))
	}

	err := h.tdb.RawMDel(del_keys)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		log.Error("del field and value failed",
			zap.String("key", string(cmd.Args[1])),
			zap.Error(err))
		conn.WriteInt(0)
		return
	}
	conn.WriteInt(len(del_keys))
}

func (h *Handler) hget(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_hash_get"
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

	key := EncodeHashKey(h.db, string(cmd.Args[1]), string(cmd.Args[2]))
	value, err := h.tdb.RawGet(key)
	if nil != err || nil == value {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		log.Error("get field and value failed",
			zap.String("key", string(cmd.Args[1])),
			zap.Error(err))
		conn.WriteNull()
		return
	}
	conn.WriteBulk(value)
}

func (h *Handler) hgetall(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_hash_getall"
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

	keyPre := EncodeHashKeyPrefix(h.db, string(cmd.Args[1]))
	keys, values, err := h.tdb.GetKeysByPrefix(keyPre, 0)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		log.Error("get fields and values failed",
			zap.String("key", string(cmd.Args[1])),
			zap.Error(err))
		conn.WriteNull()
		return
	}

	if len(keys) != len(values) {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		log.Error("len of fileds and values is not same",
			zap.String("key", string(cmd.Args[1])))
		conn.WriteNull()
		return
	}

	if 0 == len(values) {
		conn.WriteArray(0)
		return
	}

	retArr := make([][]byte, 0)
	for idx, dkey := range keys {
		ret, okey, member := DecodeHashKey(dkey)
		if !ret {
			continue
		}

		if okey != string(cmd.Args[1]) {
			log.Error("decoded key is error",
				zap.String("okey", string(cmd.Args[1])),
				zap.String("dkey", string(dkey)))
			continue
		}

		retArr = append(retArr, []byte(member))
		retArr = append(retArr, values[idx])
	}

	conn.WriteArray(len(retArr))
	for _, v := range retArr {
		conn.WriteBulk(v)
	}
}

func (h *Handler) hlen(conn redcon.Conn, cmd redcon.Command) {
	apiName := "api_hash_getlen"
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

	keyPre := EncodeHashKeyPrefix(h.db, string(cmd.Args[1]))
	keys, values, err := h.tdb.GetKeysByPrefix(keyPre, 0)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		log.Error("get keys and values failed",
			zap.String("key", string(cmd.Args[1])),
			zap.Error(err))
		conn.WriteInt(0)
		return
	}

	if len(keys) != len(values) {
		apiCounterFailedVec.WithLabelValues(apiName, h.db).Inc()
		log.Error("len of fileds and values is not same",
			zap.String("key", string(cmd.Args[1])))
		conn.WriteInt(0)
		return
	}
	conn.WriteInt(len(keys))
}
