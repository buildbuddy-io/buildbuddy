package main

type EventType string

var (
	TypeDurationBegin     EventType = "B"
	TypeDurationEnd       EventType = "E"
	TypeInstant           EventType = "i"
	TypeInstantDeprecated EventType = "I"
	TypeComplete          EventType = "X"
	TypeCounter           EventType = "C"
	TypeNestableStart     EventType = "b"
	TypeNestableInstant   EventType = "n"
	TypeNestableEnd       EventType = "e"
	TypeFlowStart         EventType = "s"
	TypeFlowStep          EventType = "t"
	TypeFlowEnd           EventType = "f"
	TypeSample            EventType = "P"
	TypeObjectCreated     EventType = "N"
	TypeObjectSnapshot    EventType = "O"
	TypeObjectDestroyed   EventType = "D"
	TypeMetadata          EventType = "M"
	TypeMemoryDumpGlobal  EventType = "V"
	TypeMemoryDumpProcess EventType = "v"
	TypeMark              EventType = "R"
	TypeClockSync         EventType = "c"
)

// BazelProfile describes Chrome's Trace Event Format(TEF)
type BazelProfile struct {
	OtherData struct {
		BuildID    string `json:"build_id,omitempty"`
		OutputBase string `json:"output_base,omitempty"`
		Date       string `json:"date,omitempty"`
	} `json:"otherData,omitempty"`

	TraceEvents []TraceEvent `json:"traceEvents,omitempty"`
}

type TraceEvent struct {
	Name      string    `json:"name,omitempty"`
	Phase     EventType `json:"ph,omitempty"`
	ProcessID int       `json:"pid,omitempty"`
	ThreadID  int       `json:"tid,omitempty"`
	Category  string    `json:"cat,omitempty"`
	TimeStamp int       `json:"ts,omitempty"`
	Duration  int       `json:"dur,omitempty"`
	ColorName string    `json:"cname,omitempty"`

	Args struct {
		Name         string `json:"name,omitempty"`
		Mnemonic     string `json:"mnemonic,omitempty"`
		ThreadID     int    `json:"tid,omitempty"`
		Action       string `json:"action,omitempty"`
		CPU          string `json:"cpu,omitempty"`
		Memory       string `json:"memory,omitempty"`
		SystemCPU    string `json:"system cpu,omitempty"`
		SystemMemory string `json:"system memory,omitempty"`
		Load         string `json:"load,omitempty"`
		// this could be either an int-as-string or an int
		SortIndex any `json:"sort_index,omitempty"`
	} `json:"args,omitempty"`
}
