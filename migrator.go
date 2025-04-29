package consistent_hash

import (
	"context"
	"errors"
	"math"
)

type Migrator func(ctx context.Context, from, to string, dataKeys map[string]struct{}) error

func (ch *consistentHash) migrateIn(ctx context.Context, virtualScore int32, nodeID string) (from, to string, datas map[string]struct{}, _err error) {
	if ch.migrator == nil {
		return
	}

	nodes, err := ch.hashRing.Node(ctx, virtualScore)

	if err != nil {
		return "", "", nil, err
	}

	if len(nodes) > 1 {
		return
	}

	lastScore, err := ch.hashRing.Floor(ctx, ch.decrScore(virtualScore))

	if err != nil {
		_err = err
		return
	}

	if lastScore == -1 || lastScore == virtualScore {
		return
	}

	nextScore, err := ch.hashRing.Ceiling(ctx, ch.incrScore(virtualScore))

	if err != nil {
		_err = err
		return
	}

	if nextScore == -1 || nextScore == virtualScore {
		return
	}

	condOne := lastScore > virtualScore
	condTwo := nextScore < virtualScore

	if condOne {
		lastScore -= math.MaxInt32
	}

	if condTwo {
		virtualScore -= math.MaxInt32
		lastScore -= math.MaxInt32
	}

	nextNodes, err := ch.hashRing.Node(ctx, nextScore)

	if err != nil {
		_err = err
		return
	}

	if len(nextNodes) == 0 {
		return
	}

	dataKeys, err := ch.hashRing.DataKeys(ctx, ch.getNodeID(nextNodes[0]))
	if err != nil {
		_err = err
		return
	}

	datas = make(map[string]struct{})

	for dataKey := range dataKeys {
		dataVirtualScore, _ := ch.encryptor.Encrypt(dataKey)
		if condOne && dataVirtualScore > (lastScore+math.MaxInt32) {
			dataVirtualScore -= math.MaxInt32
		}

		if condTwo {
			dataVirtualScore -= math.MaxInt32
		}

		if dataVirtualScore <= lastScore || dataVirtualScore > virtualScore {
			continue
		}

		datas[dataKey] = struct{}{}
	}

	if err = ch.hashRing.DeleteNodeToDataKeys(ctx, ch.getNodeID(nextNodes[0]), datas); err != nil {
		return "", "", nil, err
	}

	if err = ch.hashRing.AddNodeToDataKeys(ctx, nodeID, datas); err != nil {
		return "", "", nil, err
	}
	return ch.getNodeID(nextNodes[0]), nodeID, datas, nil
}

func (ch *consistentHash) incrScore(score int32) int32 {
	if score == math.MaxInt32-1 {
		return 0
	}
	return score + 1
}

func (ch *consistentHash) decrScore(score int32) int32 {
	if score == 0 {
		return math.MaxInt32 - 1
	}
	return score - 1
}

func (ch *consistentHash) migrateOut(ctx context.Context, virtualScore int32, nodeID string) (from, to string, datas map[string]struct{}, err error) {
	// 使用方没有注入迁移函数，则直接返回
	if ch.migrator == nil {
		return
	}

	defer func() {
		if err != nil {
			return
		}
		if to == "" || len(datas) == 0 {
			return
		}

		if err = ch.hashRing.DeleteNodeToDataKeys(ctx, nodeID, datas); err != nil {
			return
		}

		err = ch.hashRing.AddNodeToDataKeys(ctx, to, datas)
	}()

	from = nodeID

	nodes, _err := ch.hashRing.Node(ctx, virtualScore)
	if _err != nil {
		err = _err
		return
	}

	if len(nodes) == 0 {
		return
	}

	// 不是 virtualScore 下的首个节点，则无需迁移
	if ch.getNodeID(nodes[0]) != nodeID {
		return
	}

	// 如果没有数据，则直接返回
	var allDatas map[string]struct{}
	if allDatas, err = ch.hashRing.DataKeys(ctx, nodeID); err != nil {
		return
	}

	if len(allDatas) == 0 {
		return
	}

	// 遍历数据 key，找出其中属于 (lastScore, virtualScore] 范围的数据
	lastScore, _err := ch.hashRing.Floor(ctx, ch.decrScore(virtualScore))
	if _err != nil {
		err = _err
		return
	}

	var onlyScore bool
	if lastScore == -1 || lastScore == virtualScore {
		if len(nodes) == 1 {
			err = errors.New("no other no")
			return
		}
		onlyScore = true
	}

	pattern := lastScore > virtualScore
	if pattern {
		lastScore -= math.MaxInt32
	}

	datas = make(map[string]struct{})
	for data := range allDatas {
		if onlyScore {
			datas[data] = struct{}{}
			continue
		}
		dataScore, _ := ch.encryptor.Encrypt(data)
		if pattern && dataScore > lastScore+math.MaxInt32 {
			dataScore -= math.MaxInt32
		}
		if dataScore <= lastScore || dataScore > virtualScore {
			continue
		}
		datas[data] = struct{}{}
	}

	// 如果同一个 virtualScore 下存在多个节点，则直接委托给下一个节点
	if len(nodes) > 1 {
		to = ch.getNodeID(nodes[1])
		return
	}

	// 寻找后继节点
	if to, err = ch.getValidNextNode(ctx, virtualScore, nodeID, nil); err != nil {
		err = _err
		return
	}

	if to == "" {
		err = errors.New("no other node")
	}

	return
}

func (ch *consistentHash) getValidNextNode(ctx context.Context, score int32, nodeID string, ranged map[int32]struct{}) (string, error) {
	// 寻找后继节点
	nextScore, err := ch.hashRing.Ceiling(ctx, ch.incrScore(score))
	if err != nil {
		return "", err
	}
	if nextScore == -1 {
		return "", nil
	}

	if _, ok := ranged[nextScore]; ok {
		return "", nil
	}

	// 后继节点的 key 必须不与自己相同，否则继续往下寻找
	nextNodes, err := ch.hashRing.Node(ctx, nextScore)
	if err != nil {
		return "", err
	}

	if len(nextNodes) == 0 {
		return "", errors.New("next node empty")
	}

	if nextNode := ch.getNodeID(nextNodes[0]); nextNode != nodeID {
		return nextNode, nil
	}

	if len(nextNodes) > 1 {
		return ch.getNodeID(nextNodes[1]), nil
	}

	if ranged == nil {
		ranged = make(map[int32]struct{})
	}
	ranged[score] = struct{}{}

	// 寻找下一个目标
	return ch.getValidNextNode(ctx, nextScore, nodeID, ranged)
}
