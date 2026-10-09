package monitor

import "github.com/yunnnnn1/go-helix/pkg/metrics"

// failoverCounter 按结果（success / failed /
// aborted）统计切换流程次数。"aborted" 表示必需的围栏闸门阻止了提升。
var failoverCounter = metrics.Counter(
	"helix_failover_total",
	"Number of failover/switchover sequences by result",
	"result",
)
