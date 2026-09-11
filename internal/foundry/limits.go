package foundry

const (
	MaxPromptBytes              = 4 << 20
	MaxToolSchemaBytes          = 2 << 20
	MaxIdentifierBytes          = 4 << 10
	DefaultMaxOutputBytes       = 1 << 20
	DefaultMaxStreamBytes       = 16 << 20
	DefaultMaxEventBytes        = 8 << 20
	DefaultMaxBrokeredBytes     = 4 << 20
	DefaultMaxBrokeredTurnBytes = 16 << 20
	DefaultMaxBrokeredCalls     = 256
	DefaultMaxEvents            = 4096
)
