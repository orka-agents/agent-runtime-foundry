package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestACPStructFieldsKeepOpaqueValuesCaseSensitive(t *testing.T) {
	var value struct {
		foundryOutputItem
		Status string         `json:"status"`
		Map    map[string]any `json:"map"`
		Any    any            `json:"any"`
	}
	data := []byte(`{"TyPe":"function_call","ſtatus":"completed","ArGuMeNtS":{"Key":1,"key":2},"Map":{"Key":3,"key":4},"Any":{"Key":5,"key":6},"unknown":{"Key":7,"key":8},"Unknown":null}`)
	if err := acpDecodeStruct(data, &value, false); err != nil {
		t.Fatal("single aliases or opaque case-sensitive values rejected")
	}
	if value.Type != "function_call" || value.Status != "completed" ||
		string(value.Arguments) != `{"Key":1,"key":2}` || len(value.Map) != 2 || len(value.Any.(map[string]any)) != 2 {
		t.Fatal("structured decode changed an opaque value")
	}
	for _, data := range []string{
		`{"arguments":{"Key":1,"Key":2}}`,
		`{"Map":{"Key":1,"\u004bey":2}}`,
		`{"Any":{"nested":{"same":1,"same":2}}}`,
		`{"unknown":{"same":1,"same":2}}`,
		`{"arguments":"\ud800"}`,
	} {
		if acpDecodeStruct([]byte(data), &value, false) == nil {
			t.Error("opaque value bypassed existing exact duplicate or Unicode validation")
		}
	}
}

func TestACPStructFieldsRejectAliasesBeforeApplyingValues(t *testing.T) {
	type nested struct {
		Status string `json:"status"`
	}
	for _, data := range []string{
		`{"status":"active","Status":"idle"}`,
		`{"status":"active","ſtatus":"idle"}`,
		`{"status":"active","\u0053tatus":"idle"}`,
		`{"status":null,"Status":"idle"}`,
		`{"values":[{"status":"active","Status":"idle"}]}`,
		`{"pointer":{"status":"active","Status":"idle"}}`,
		`{"pointer":{"status":"active"},"Pointer":{"status":"idle"}}`,
	} {
		var value struct {
			nested
			Values  []nested `json:"values"`
			Pointer *nested  `json:"pointer"`
		}
		value.Status = "untouched"
		if acpDecodeStruct([]byte(data), &value, false) == nil || value.Status != "untouched" || value.Pointer != nil || value.Values != nil {
			t.Error("folded alias was accepted or applied before rejection")
		}
	}
}

func TestACPStructFieldsPreserveDecoderCompatibility(t *testing.T) {
	type embedded struct {
		ID string `json:"id"`
	}
	for _, data := range []string{
		`{"ID":"first","id":"second","Status":"completed"}`,
		`{"Id":"folded","status":"completed"}`,
		`{"id":"canonical","unknown":{"Status":"active","status":"idle"}}`,
	} {
		var ordinary, checked struct {
			embedded
			ID     string `json:"ID"`
			Status string `json:"status"`
		}
		if acpDecode([]byte(data), &ordinary, false) != nil || acpDecodeStruct([]byte(data), &checked, false) != nil || ordinary != checked {
			t.Error("unambiguous fields differ from encoding/json")
		}
	}
	var object struct {
		Value json.RawMessage `json:"value"`
	}
	if acpDecodeStruct([]byte(`{"unknown":true}`), &object, false) != nil || acpDecodeStruct([]byte(`{"unknown":true}`), &object, true) == nil {
		t.Error("unknown-field policy changed")
	}
	if acpDecodeStruct([]byte(`{"value":`+strings.Repeat("[", 65)+strings.Repeat("]", 65)+`}`), &object, false) == nil {
		t.Error("structured decoder lost the depth bound")
	}
}

func TestACPStructFieldsSingleAliasesPreserveResponseAndArguments(t *testing.T) {
	data := []byte(`{"ID":"response-1","ſtatus":"completed","OuTpUt":[{"ID":"item-1","TyPe":"function_call","ſtatus":"completed","Name":"probe","CALL_ID":"call-1","Arguments":{"Key":1,"key":2}}]}`)
	response, err := acpDecodeFoundryResponse(data)
	if err != nil || response.Status != "completed" || len(response.Output) != 1 ||
		response.Output[0].Type != "function_call" || !bytes.Equal(response.Output[0].Arguments, []byte(`{"Key":1,"key":2}`)) {
		t.Fatal("single response/item aliases changed case-sensitive arguments")
	}
}

func TestBrokerResponseEvidenceKeepsUnusableOutputOwnership(t *testing.T) {
	const data = `{"id":"response-1","status":"completed","agent_session_id":"owned","output":[{"type":"web_search_call","Type":"function_call"}]}`
	response, err := brokerDecodeResponseEvidence([]byte(data))
	if err != nil || response.ID != "response-1" || response.AgentSessionID != "owned" {
		t.Fatal("coherent response ownership was discarded because output is unusable")
	}
	if _, err := acpDecodeFoundryResponse([]byte(data)); err == nil {
		t.Fatal("ownership evidence admitted unusable output")
	}
	for _, data := range []string{
		`{"id":"wrong","ID":"response-1","status":"completed"}`,
		`{"id":"response-1","status":"active","Status":"completed"}`,
		`{"id":"response-1","status":"completed","agent_session_id":"wrong","Agent_Session_ID":"owned"}`,
		`{"id":"response-1","status":"completed","error":{"code":"failure"},"Error":null}`,
	} {
		if _, err := brokerDecodeResponseEvidence([]byte(data)); err == nil {
			t.Error("contradictory fields fabricated response ownership")
		}
	}
}
