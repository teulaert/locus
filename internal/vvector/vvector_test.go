package vvector

import (
	"testing"
)

func TestCompare(t *testing.T) {
	tests := []struct {
		name string
		a, b Vector
		want Relation
	}{
		{"equal empty", Vector{}, Vector{}, Equal},
		{"equal same", Vector{"a": 1, "b": 2}, Vector{"b": 2, "a": 1}, Equal},
		{"a before", Vector{"a": 1}, Vector{"a": 2}, Before},
		{"a after", Vector{"a": 2}, Vector{"a": 1}, After},
		{"ancestor subset", Vector{"a": 1}, Vector{"a": 1, "b": 1}, Before},
		{"divergent", Vector{"a": 2, "b": 1}, Vector{"a": 1, "b": 2}, Divergent},
		{"divergent disjoint", Vector{"a": 1}, Vector{"b": 1}, Divergent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Compare(tc.a, tc.b); got != tc.want {
				t.Fatalf("Compare(%v,%v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestIncrementDoesNotMutate(t *testing.T) {
	base := Vector{"a": 1}
	next := base.Increment("a")
	if base["a"] != 1 || next["a"] != 2 {
		t.Fatalf("increment mutated or wrong: base=%v next=%v", base, next)
	}
}

func TestMergeMax(t *testing.T) {
	got := MergeMax(Vector{"a": 3, "b": 1}, Vector{"a": 2, "b": 5, "c": 1})
	want := Vector{"a": 3, "b": 5, "c": 1}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("MergeMax = %v, want %v", got, want)
		}
	}
}

func TestStringParseRoundTrip(t *testing.T) {
	v := Vector{"peer-b": 2, "peer-a": 1}
	s := v.String()
	if s != "peer-a:1,peer-b:2" {
		t.Fatalf("non-canonical string: %s", s)
	}
	back, err := Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	if Compare(v, back) != Equal {
		t.Fatalf("round trip changed value: %v vs %v", v, back)
	}
	if _, err := Parse("garbage"); err == nil {
		t.Fatal("expected parse error")
	}
	if _, err := Parse("peer:xx"); err == nil {
		t.Fatal("expected counter parse error")
	}
	empty, err := Parse("")
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty parse: %v %v", empty, err)
	}
}

func TestRelationString(t *testing.T) {
	for r, want := range map[Relation]string{Equal: "equal", Before: "before", After: "after", Divergent: "divergent"} {
		if r.String() != want {
			t.Fatalf("Relation(%d).String() = %q, want %q", r, r.String(), want)
		}
	}
}
