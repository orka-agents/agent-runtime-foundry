package main

import (
	"encoding/json"
	"reflect"
	"strings"
)

type acpJSONStructField struct {
	name   string
	typeOf reflect.Type
	depth  int
	tagged bool
}

func acpJSONStructType(value reflect.Type) reflect.Type {
	for value != nil && value.Kind() == reflect.Pointer {
		value = value.Elem()
	}
	if value == nil || reflect.PointerTo(value).Implements(reflect.TypeFor[json.Unmarshaler]()) {
		return nil
	}
	return value
}

// Only struct fields define folded names. In particular, RawMessage tool
// arguments, map keys and unknown metadata retain their case-sensitive JSON.
func acpJSONStructFields(value reflect.Type, depth int) ([]acpJSONStructField, error) {
	if depth > 64 {
		return nil, acpInvalidParams
	}
	value = acpJSONStructType(value)
	if value == nil || value.Kind() != reflect.Struct {
		return nil, nil
	}
	var candidates []acpJSONStructField
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "-" || (!field.IsExported() && !field.Anonymous) {
			continue
		}
		if name == "" && field.Anonymous {
			if embedded := acpJSONStructType(field.Type); embedded != nil && embedded.Kind() == reflect.Struct {
				fields, err := acpJSONStructFields(field.Type, depth+1)
				if err != nil {
					return nil, err
				}
				candidates = append(candidates, fields...)
				continue
			}
		}
		if !field.IsExported() {
			continue
		}
		tagged := name != ""
		if name == "" {
			name = field.Name
		}
		candidates = append(candidates, acpJSONStructField{name, field.Type, depth, tagged})
	}
	// Match encoding/json's dominance rules for the selected plain structs:
	// shallower fields win; at equal depth a tagged field wins; ties are ignored.
	var fields []acpJSONStructField
	for i, candidate := range candidates {
		keep := true
		for j, other := range candidates {
			if i == j || candidate.name != other.name {
				continue
			}
			if other.depth < candidate.depth || (other.depth == candidate.depth && (other.tagged || !candidate.tagged)) {
				keep = false
				break
			}
		}
		if keep {
			fields = append(fields, candidate)
		}
	}
	return fields, nil
}

func acpJSONMatchField(fields []acpJSONStructField, name string) (string, reflect.Type) {
	for _, field := range fields {
		if field.name == name {
			return field.name, field.typeOf
		}
	}
	for _, field := range fields {
		if strings.EqualFold(field.name, name) {
			return field.name, field.typeOf
		}
	}
	return name, nil
}
