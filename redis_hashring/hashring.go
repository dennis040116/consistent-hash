package redis_hashring

import (
	"consistent_hash"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/demdxx/gocast"
	"github.com/gomodule/redigo/redis"
	"github.com/xiaoxuxiansheng/redis_lock"
)

type rHashRing struct {
	key         string
	redisClient *Client
}

func NewRHashRing(key string, redisClient *Client) consistent_hash.HashRing {
	return &rHashRing{
		key:         key,
		redisClient: redisClient,
	}
}

func (hr *rHashRing) getLockKey() string {
	return fmt.Sprintf("redis:consistent_hash:hashring:lock:%s", hr.key)
}

func (hr *rHashRing) getRingKey() string {
	return fmt.Sprintf("redis:consistent_hash:hashring:%s", hr.key)
}

func (hr *rHashRing) getNodeReplicasKey() string {
	return fmt.Sprintf("redis:consistent_hash:node:replicas:%s", hr.key)
}

func (hr *rHashRing) getNodeDataKey(node string) string {
	return fmt.Sprintf("redis:consistent_hash:node:data:%s", node)
}

func (hr *rHashRing) Lock(ctx context.Context, expireSeconds int) error {
	lock := redis_lock.NewRedisLock(hr.getLockKey(), hr.redisClient, redis_lock.WithExpireSeconds(int64(expireSeconds)))
	return lock.Lock(ctx)
}

func (hr *rHashRing) Unlock(ctx context.Context) error {
	lock := redis_lock.NewRedisLock(hr.getLockKey(), hr.redisClient)
	return lock.Unlock(ctx)
}

func (hr *rHashRing) Add(ctx context.Context, virtualScore int32, nodeID string) error {
	scoreEntities, err := hr.redisClient.ZRangeByScore(ctx, hr.getRingKey(), int64(virtualScore), int64(virtualScore))
	if err != nil {
		return fmt.Errorf("add node to hashring failed, err: %w", err)
	}

	if len(scoreEntities) > 1 {
		return fmt.Errorf("invalid len of entity: %d", len(scoreEntities))
	}

	var nodeIDs []string
	if len(scoreEntities) == 1 {
		if err = json.Unmarshal([]byte(scoreEntities[0].Val), &nodeIDs); err != nil {
			return fmt.Errorf("unmarshal nodeIDs failed, err: %w", err)
		}

		for _, node := range nodeIDs {
			if node == nodeID {
				return nil
			}
		}

		if err = hr.redisClient.ZRem(ctx, hr.getRingKey(), scoreEntities[0].Score); err != nil {
			return fmt.Errorf("remove node from hashring failed, err: %w", err)
		}

		nodeIDs = append(nodeIDs, nodeID)
		nodeIDs, _ := json.Marshal(nodeIDs)
		if err = hr.redisClient.ZAdd(ctx, hr.getRingKey(), int64(virtualScore), string(nodeIDs)); err != nil {
			return fmt.Errorf("add node to hashring failed, err: %w", err)
		}
	}
	return nil
}

func (hr *rHashRing) Ceiling(ctx context.Context, virtualScore int32) (int32, error) {
	scoreEntity, err := hr.redisClient.Ceiling(ctx, hr.getRingKey(), int64(virtualScore))
	if err != nil && !errors.Is(err, ErrScoreNotExist) {
		return 0, fmt.Errorf("get ceiling node failed, err: %w", err)
	}

	if scoreEntity != nil {
		return int32(scoreEntity.Score), nil
	}

	if scoreEntity, err = hr.redisClient.FirstOrLast(ctx, hr.getRingKey(), true); err != nil && !errors.Is(err, ErrScoreNotExist) {
		return 0, fmt.Errorf("get first node failed, err: %w", err)
	}

	if scoreEntity != nil {
		return int32(scoreEntity.Score), nil
	}

	return -1, nil
}

func (hr *rHashRing) Floor(ctx context.Context, virtualScore int32) (int32, error) {
	scoreEntity, err := hr.redisClient.Floor(ctx, hr.getRingKey(), int64(virtualScore))
	if err != nil && !errors.Is(err, ErrScoreNotExist) {
		return 0, fmt.Errorf("get floor node failed, err: %w", err)
	}

	if scoreEntity != nil {
		return int32(scoreEntity.Score), nil
	}

	if scoreEntity, err = hr.redisClient.FirstOrLast(ctx, hr.getRingKey(), false); err != nil && !errors.Is(err, ErrScoreNotExist) {
		return 0, fmt.Errorf("get last node failed, err: %w", err)
	}

	if scoreEntity != nil {
		return int32(scoreEntity.Score), nil
	}

	return -1, nil
}

func (hr *rHashRing) Rem(ctx context.Context, virtualScore int32, nodeID string) error {
	scoreEntity, err := hr.redisClient.ZRangeByScore(ctx, hr.getRingKey(), int64(virtualScore), int64(virtualScore))
	if err != nil {
		return fmt.Errorf("get node from hashring failed, err: %w", err)
	}

	if len(scoreEntity) != 1 {
		return fmt.Errorf("invalid len of entity: %d, err: %w", len(scoreEntity), ErrScoreNotExist)
	}

	var nodeIDs []string

	if err = json.Unmarshal([]byte(scoreEntity[0].Val), &nodeIDs); err != nil {
		return fmt.Errorf("unmarshal nodeIDs failed, err: %w", err)
	}

	index := -1
	for i, node := range nodeIDs {
		if node == nodeID {
			index = i
			break
		}
	}

	if index == -1 {
		return nil
	}

	if err = hr.redisClient.ZRem(ctx, hr.getRingKey(), scoreEntity[0].Score); err != nil {
		return fmt.Errorf("remove node from hashring failed, err: %w", err)
	}

	nodeIDs = append(nodeIDs[:index], nodeIDs[index+1:]...)
	if len(nodeIDs) == 0 {
		return nil
	}

	newNodeIDStr, _ := json.Marshal(nodeIDs)
	if err = hr.redisClient.ZAdd(ctx, hr.getRingKey(), int64(virtualScore), string(newNodeIDStr)); err != nil {
		return fmt.Errorf("add node to hashring failed, err: %w", err)
	}

	return nil
}

func (hr *rHashRing) Nodes(ctx context.Context) (map[string]int, error) {
	rawData, err := hr.redisClient.HGetAll(ctx, hr.getNodeReplicasKey())
	if err != nil {
		return nil, fmt.Errorf("get node replicas failed, err: %w", err)
	}

	data := make(map[string]int, len(rawData))

	for rawKey, rawValue := range rawData {
		data[rawKey] = gocast.ToInt(rawValue)
	}
	return data, nil
}

func (hr *rHashRing) AddNodeToReplica(ctx context.Context, nodeID string, replicas int) error {
	if err := hr.redisClient.HSet(ctx, hr.getNodeReplicasKey(), nodeID, gocast.ToString(replicas)); err != nil {
		return fmt.Errorf("add node to replicas failed, err: %w", err)
	}
	return nil
}

func (hr *rHashRing) DeleteNodeToReplica(ctx context.Context, nodeID string) error {
	if err := hr.redisClient.HDel(ctx, hr.getNodeReplicasKey(), nodeID); err != nil {
		return fmt.Errorf("delete node from replicas failed, err: %w", err)
	}
	return nil
}

func (hr *rHashRing) Node(ctx context.Context, virtualScore int32) ([]string, error) {
	scoreEntity, err := hr.redisClient.ZRangeByScore(ctx, hr.getRingKey(), int64(virtualScore), int64(virtualScore))
	if err != nil {
		return nil, fmt.Errorf("get node from hashring failed, err: %w", err)
	}

	if len(scoreEntity) != 1 {
		return nil, fmt.Errorf("invalid len of entity: %d, err: %w", len(scoreEntity), ErrScoreNotExist)
	}

	var nodeIDs []string
	if err = json.Unmarshal([]byte(scoreEntity[0].Val), &nodeIDs); err != nil {
		return nil, fmt.Errorf("unmarshal nodeIDs failed, err: %w", err)
	}

	return nodeIDs, nil
}

func (hr *rHashRing) DataKeys(ctx context.Context, nodeID string) (map[string]struct{}, error) {
	dataStr, err := hr.redisClient.Get(ctx, hr.getNodeDataKey(nodeID))
	if err != nil && !errors.Is(err, redis.ErrNil) {
		return nil, fmt.Errorf("get node data keys failed, err: %w", err)
	}

	dataKeys := make(map[string]struct{})
	if len(dataStr) > 0 {
		if err = json.Unmarshal([]byte(dataStr), &dataKeys); err != nil {
			return nil, fmt.Errorf("unmarshal node data keys failed, err: %w", err)
		}
	}
	return dataKeys, nil
}

func (hr *rHashRing) AddNodeToDataKeys(ctx context.Context, nodeID string, dataKeys map[string]struct{}) error {
	dataStr, err := hr.redisClient.Get(ctx, hr.getNodeDataKey(nodeID))
	if err != nil && !errors.Is(err, redis.ErrNil) {
		return fmt.Errorf("get node data keys failed, err: %w", err)
	}

	var oldDataKeys map[string]struct{}
	if len(dataStr) > 0 {
		if err = json.Unmarshal([]byte(dataStr), &oldDataKeys); err != nil {
			return fmt.Errorf("unmarshal node data keys failed, err: %w", err)
		}
	}

	if oldDataKeys == nil {
		oldDataKeys = make(map[string]struct{})
	}

	for key := range dataKeys {
		oldDataKeys[key] = struct{}{}
	}

	dataKeyStr, _ := json.Marshal(oldDataKeys)
	if err = hr.redisClient.Set(ctx, hr.getNodeDataKey(nodeID), string(dataKeyStr)); err != nil {
		return fmt.Errorf("set node data keys failed, err: %w", err)
	}
	return nil
}

func (hr *rHashRing) DeleteNodeToDataKeys(ctx context.Context, nodeID string, dataKeys map[string]struct{}) error {
	dataStr, err := hr.redisClient.Get(ctx, hr.getNodeDataKey(nodeID))
	if err != nil && !errors.Is(err, redis.ErrNil) {
		return fmt.Errorf("get node data keys failed, err: %w", err)
	}

	if len(dataStr) == 0 {
		return nil
	}

	var oldDataKeys map[string]struct{}
	if err = json.Unmarshal([]byte(dataStr), &oldDataKeys); err != nil {
		return fmt.Errorf("unmarshal node data keys failed, err: %w", err)
	}

	for key := range dataKeys {
		delete(oldDataKeys, key)
	}

	dataKeyStr, _ := json.Marshal(oldDataKeys)
	if err = hr.redisClient.Set(ctx, hr.getNodeDataKey(nodeID), string(dataKeyStr)); err != nil {
		return fmt.Errorf("set node data keys failed, err: %w", err)
	}
	return nil
}
