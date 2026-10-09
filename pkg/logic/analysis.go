package logic

// AnalysisCode 标识监控系统检测到的故障类型。
// 参照 orchestrator 的分析码设计，并针对 go-helix 做了调整。
type AnalysisCode string

const (
	// 主库故障
	DeadMaster              AnalysisCode = "DeadMaster"
	DeadMasterAndSlaves     AnalysisCode = "DeadMasterAndSlaves"
	DeadMasterAndSomeSlaves AnalysisCode = "DeadMasterAndSomeSlaves"
	UnreachableMaster       AnalysisCode = "UnreachableMaster"
	NonWriteableMaster      AnalysisCode = "NonWriteableMaster"
	LockedSemiSyncMaster    AnalysisCode = "LockedSemiSyncMaster"
	MasterDiskFull          AnalysisCode = "MasterDiskFull"

	// 中间主故障
	DeadIntermediateMaster AnalysisCode = "DeadIntermediateMaster"

	// 计划内操作
	GracefulMasterTakeover AnalysisCode = "GracefulMasterTakeover"
)

// RecoveryType 表示恢复过程中采用的复制方式。
type RecoveryType string

const (
	RecoveryGTID      RecoveryType = "RecoveryGTID"
	RecoveryBinlogPos RecoveryType = "RecoveryBinlogPos"
)

// AnalysisResult 描述拓扑分析的结果。
type AnalysisResult struct {
	Code         AnalysisCode
	RecoveryType RecoveryType
	Description  string
	IsActionable bool
}
