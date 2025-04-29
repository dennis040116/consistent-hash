package consistent_hash

type ConsistentHashOptions struct {
	lockExpireSeconds int
	replicas          int
}

type ConsistentHashOption func(*ConsistentHashOptions)

func repair(opts *ConsistentHashOptions) {
	if opts.lockExpireSeconds <= 0 {
		opts.lockExpireSeconds = 15
	}
	if opts.replicas <= 0 {
		opts.replicas = 5
	}
}

func WithExpireSeconds(seconds int) ConsistentHashOption {
	return func(opts *ConsistentHashOptions) {
		opts.lockExpireSeconds = seconds
	}
}

func WithReplicas(replicas int) ConsistentHashOption {
	return func(opts *ConsistentHashOptions) {
		opts.replicas = replicas
	}
}
