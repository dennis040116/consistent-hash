package consistent_hash

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

type ConsistentHash interface {
	AddNode(ctx context.Context, node string, weight int) error
	RemoveNode(ctx context.Context, node string) error
	GetNode(ctx context.Context, key string) (string, error)
}

type consistentHash struct {
	hashRing  HashRing
	migrator  Migrator
	encryptor Encryptor
	opts      ConsistentHashOptions
}

func NewConsistentHash(hr HashRing, m Migrator, enc Encryptor, opts ...ConsistentHashOption) ConsistentHash {
	ch := consistentHash{
		hashRing:  hr,
		migrator:  m,
		encryptor: enc,
	}
	for _, opt := range opts {
		opt(&ch.opts)
	}

	repair(&ch.opts)

	return &ch
}

func (ch *consistentHash) AddNode(ctx context.Context, nodeID string, weight int) error {
	// 1 加全局分布式锁
	if err := ch.hashRing.Lock(ctx, ch.opts.lockExpireSeconds); err != nil {
		return err
	}

	defer func() {
		_ = ch.hashRing.Unlock(ctx)
	}()

	// 2 如果节点已经存在了，直接返回重复创建的错误
	nodes, err := ch.hashRing.Nodes(ctx)
	if err != nil {
		return err
	}

	for node := range nodes {
		if node == nodeID {
			return errors.New("repeat node")
		}
	}

	// 3 根据 replicas 配置，计算出使用的虚拟节点个数
	replicas := ch.getValidWeight(weight) * ch.opts.replicas
	// 4. 将计算得到的 replicas 个数与 nodeID 的映射关系放到 hash ring 中，同时也能标识出当前 nodeID 已经存在
	if err = ch.hashRing.AddNodeToReplica(ctx, nodeID, replicas); err != nil {
		return err
	}

	var migrateTasks []func()
	for i := 0; i < replicas; i++ {
		// 5 使用 encryptor，推算出对应的 k 个虚拟节点的数值
		nodeKey := ch.getRawNodeKey(nodeID, i)
		virtualScore, _ := ch.encryptor.Encrypt(nodeKey)

		// 6 批量执行，将对应的虚拟节点添加到 hash ring 当中
		if err := ch.hashRing.Add(ctx, virtualScore, nodeKey); err != nil {
			return err
		}

		// 7 调用 migrateIn 方法，获取到当前这个 virtualScore 的添加操作，会导致有哪些数据需要从哪个节点迁移到哪个节点
		// from: 数据迁移起点的节点 id
		// to: 数据迁移终点的节点 id
		// data: 需要迁移的数据的 key
		from, to, datas, err := ch.migrateIn(ctx, virtualScore, nodeID)
		if err != nil {
			return err
		}

		// 无数据需要迁移，则直接跳过
		if len(datas) == 0 {
			continue
		}

		// 创建数据迁移任务，但不是立即执行，而是放在方法返回前统一批量执行
		migrateTasks = append(migrateTasks, func() {
			_ = ch.migrator(ctx, from, to, datas)
		})
	}

	ch.batchExecuteMigrator(migrateTasks)

	return nil
}

// 删除节点需要触发数据迁移，
// 作为使用方，需要知道，有哪些数据需要完成迁移，从哪里迁移到哪里
func (ch *consistentHash) RemoveNode(ctx context.Context, nodeID string) error {
	// 1 加全局分布式锁
	if err := ch.hashRing.Lock(ctx, ch.opts.lockExpireSeconds); err != nil {
		return err
	}

	defer func() {
		_ = ch.hashRing.Unlock(ctx)
	}()

	// 2 如果节点不存在，直接返回失败
	nodes, err := ch.hashRing.Nodes(ctx)
	if err != nil {
		return err
	}

	var (
		nodeExist bool
		replicas  int
	)
	for node, _replicas := range nodes {
		if node == nodeID {
			nodeExist = true
			replicas = _replicas
			break
		}
	}

	if !nodeExist {
		return errors.New("invalid node id")
	}

	if err = ch.hashRing.DeleteNodeToReplica(ctx, nodeID); err != nil {
		return err
	}

	var migrateTasks []func()
	// 3 根据 replicas，计算出使用的虚拟节点个数
	for i := 0; i < replicas; i++ {
		// 4 使用 encryptor，推算出对应的 k 个虚拟节点数值
		virtualScore, _ := ch.encryptor.Encrypt(fmt.Sprintf("%s_%d", nodeID, i))
		// 5 批量执行节点删除操作，如果涉及到数据迁移操作，调用 migrator
		from, to, datas, err := ch.migrateOut(ctx, virtualScore, nodeID)
		if err != nil {
			return err
		}

		nodeKey := ch.getRawNodeKey(nodeID, i)
		if err = ch.hashRing.Rem(ctx, virtualScore, nodeKey); err != nil {
			return err
		}

		if len(datas) == 0 {
			continue
		}

		// 创建数据迁移任务，但不是立即执行，而是放在方法返回前统一批量执行
		migrateTasks = append(migrateTasks, func() {
			_ = ch.migrator(ctx, from, to, datas)
		})

	}

	ch.batchExecuteMigrator(migrateTasks)

	return nil
}

func (ch *consistentHash) batchExecuteMigrator(migrateTasks []func()) {
	// 执行所有的数据迁移任务
	var wg sync.WaitGroup
	for _, migrateTask := range migrateTasks {
		// shadow
		migrateTask := migrateTask
		wg.Add(1)
		go func() {
			defer func() {
				if err := recover(); err != nil {

				}
				wg.Done()
			}()
			migrateTask()
		}()
	}
	wg.Wait()
}

func (ch *consistentHash) GetNode(ctx context.Context, dataKey string) (string, error) {
	// 1 加全局分布式锁
	if err := ch.hashRing.Lock(ctx, ch.opts.lockExpireSeconds); err != nil {
		return "", err
	}

	defer func() {
		_ = ch.hashRing.Unlock(ctx)
	}()

	// 1 输入一个数据 key，查询其所属的节点 id
	dataScore, _ := ch.encryptor.Encrypt(dataKey)
	ceilingScore, err := ch.hashRing.Ceiling(ctx, dataScore)
	if err != nil {
		return "", err
	}

	if ceilingScore == -1 {
		return "", errors.New("no node available")
	}

	nodes, err := ch.hashRing.Node(ctx, ceilingScore)
	if err != nil {
		return "", err
	}

	if len(nodes) == 0 {
		return "", errors.New("no node available with empty score")
	}

	// 2 在这个过程中会建立这则数据与节点 id 的映射关系
	if err = ch.hashRing.AddNodeToDataKeys(ctx, ch.getNodeID(nodes[0]), map[string]struct{}{
		dataKey: {},
	}); err != nil {
		return "", err
	}

	return nodes[0], nil
}

func (ch *consistentHash) getValidWeight(weight int) int {
	if weight <= 0 {
		return 1
	}

	if weight >= 10 {
		return 10
	}

	return weight
}

func (ch *consistentHash) getRawNodeKey(nodeID string, index int) string {
	return fmt.Sprintf("%s_%d", nodeID, index)
}

func (ch *consistentHash) getNodeID(rawNodeKey string) string {
	index := strings.LastIndex(rawNodeKey, "_")
	return rawNodeKey[:index]
}
