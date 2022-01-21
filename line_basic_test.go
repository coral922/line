package line

import (
	"sort"
	"testing"
)

func emptyStage(name string) *Stage {
	return NewStage(name, func(ctx ExecContext, input *M) (output *M, err error) {
		return input, nil
	})
}

func checkKeys(l *Line, keys []string) bool {
	ks := make([]string, 0)
	for k, v := range l.stages {
		if k != v.name {
			return false
		}
		ks = append(ks, k)
	}
	if len(ks) != len(keys) {
		return false
	}
	sort.Strings(keys)
	sort.Strings(ks)
	for i := range ks {
		if ks[i] != keys[i] {
			return false
		}
	}
	return true
}

func checkSort(l *Line, stages ...*Stage) bool {
	f := l.first
	if f != nil && f.pre != nil {
		return false
	}
	index := 0
	for f != nil {
		if len(stages) < index+1 {
			return false
		}
		if f != stages[index] {
			return false
		}
		f = f.next
		index++
	}
	if index != len(stages) {
		return false
	}
	if len(stages) != 0 && stages[len(stages)-1].next != nil {
		return false
	}
	return true
}

func TestLine_SetStages_Inactive(t *testing.T) {
	l := New()
	s1, s2, s3 := emptyStage("s1"), emptyStage("s2"), emptyStage("s3")
	l.SetStages([]*Stage{s1, s2, s3})
	if !checkKeys(l, []string{"s1", "s2", "s3"}) {
		t.FailNow()
	}
	if !checkSort(l, s1, s2, s3) {
		t.FailNow()
	}
	s4 := emptyStage("s4")
	l.SetStages([]*Stage{s3, s4})
	if !checkKeys(l, []string{"s3", "s4"}) || !checkSort(l, s3, s4) {
		t.FailNow()
	}
	l.SetStages([]*Stage{})
	if !checkKeys(l, []string{}) || !checkSort(l) {
		t.FailNow()
	}
}

func TestLine_AppendStages_Inactive(t *testing.T) {
	l := New()
	s1, s2, s3 := emptyStage("s1"), emptyStage("s2"), emptyStage("s3")
	l.AppendStages([]*Stage{s2, s3})
	if !checkKeys(l, []string{"s2", "s3"}) || !checkSort(l, s2, s3) {
		t.FailNow()
	}
	l.SetStages([]*Stage{s1, s2})
	s4, s5, s6, s7, s8 := emptyStage("s4"), emptyStage("s5"), emptyStage("s6"), emptyStage("s7"), emptyStage("s8")
	s9 := emptyStage("s9")
	l.AppendStages([]*Stage{s3, s4})
	if !checkKeys(l, []string{"s1", "s2", "s3", "s4"}) || !checkSort(l, s1, s2, s3, s4) {
		t.FailNow()
	}
	l.AppendStages([]*Stage{s5, s6}, "")
	if !checkKeys(l, []string{"s5", "s6", "s1", "s2", "s3", "s4"}) || !checkSort(l, s5, s6, s1, s2, s3, s4) {
		t.FailNow()
	}
	l.AppendStages([]*Stage{s7, s8}, "s1")
	if !checkKeys(l, []string{"s5", "s6", "s1", "s7", "s8", "s2", "s3", "s4"}) || !checkSort(l, s5, s6, s1, s7, s8, s2, s3, s4) {
		t.FailNow()
	}
	l.AppendStages([]*Stage{s9}, "not exist")
	if !checkKeys(l, []string{"s5", "s6", "s1", "s7", "s8", "s2", "s3", "s4", "s9"}) || !checkSort(l, s5, s6, s1, s7, s8, s2, s3, s4, s9) {
		t.FailNow()
	}
}

func TestLine_RemoveStage_Inactive(t *testing.T) {
	l := New()
	s4, s5, s6, s7, s8 := emptyStage("s4"), emptyStage("s5"), emptyStage("s6"), emptyStage("s7"), emptyStage("s8")
	l.SetStages([]*Stage{s4, s5, s6, s7, s8})
	l.RemoveStage("not exist")
	if !checkKeys(l, []string{"s4", "s5", "s6", "s7", "s8"}) || !checkSort(l, s4, s5, s6, s7, s8) {
		t.FailNow()
	}
	l.RemoveStage("s4")
	if !checkKeys(l, []string{"s5", "s6", "s7", "s8"}) || !checkSort(l, s5, s6, s7, s8) {
		t.FailNow()
	}
	l.RemoveStage("s6")
	if !checkKeys(l, []string{"s5", "s7", "s8"}) || !checkSort(l, s5, s7, s8) {
		t.FailNow()
	}
	l.RemoveStage("s8")
	if !checkKeys(l, []string{"s5", "s7"}) || !checkSort(l, s5, s7) {
		t.FailNow()
	}
	l.RemoveStage("s5")
	l.RemoveStage("s7")
	if !checkKeys(l, []string{}) || !checkSort(l) {
		t.FailNow()
	}
}

func TestLine_GetStage(t *testing.T) {
	l := New()
	s1, s2, s3 := emptyStage("s1"), emptyStage("s2"), emptyStage("s3")
	l.SetStages([]*Stage{s1, s2, s3})
	if l.GetStage("s1") != s1 ||
		l.GetStage("s2") != s2 ||
		l.GetStage("s3") != s3 ||
		l.GetStage("not exist") != nil {
		t.FailNow()
	}
}

func TestLine_Run_NoInput(t *testing.T) {
	l := New()
	t.Run("empty stage", func(t *testing.T) {
		defer func() {
			p := recover()
			if p != ErrNoStage {
				t.FailNow()
			}
		}()
		l.Run()
		t.FailNow()
	})
	s1, s2, s3 := emptyStage("s1"), emptyStage("s2"), emptyStage("s3")
	l.SetStages([]*Stage{s1, s2, s3})
	l.Run()
	if !l.opened.Val() {
		t.FailNow()
	}
	f := l.first
	for f != nil {
		if !f.active.Val() {
			t.FailNow()
		}
		f = f.next
	}
	select {
	case _, ok := <-l.sigInputLoopExited:
		if !ok {
			t.FailNow()
		}
	default:
	}
}

func TestLine_StopAndWait_NoInput(t *testing.T) {
	l := New()
	s1, s2, s3 := emptyStage("s1"), emptyStage("s2"), emptyStage("s3")
	l.SetStages([]*Stage{s1, s2, s3}).Run()
	s := l.StopAndWait()
	if !s {
		t.FailNow()
	}
}

func TestLine_Input(t *testing.T) {
	type obj struct {
		id string
	}
	l := New()
	s1, s2, s3 := emptyStage("s1"), emptyStage("s2"), emptyStage("s3")
	l.SetStages([]*Stage{s1, s2, s3})
	t.Run("general input", func(t *testing.T) {
		l.Input(obj{"1"})
		l.Input(obj{"2"})
		l.Input(obj{"3"})
		if l.q.Len() != 3 {
			t.FailNow()
		}
	})
	t.Run("batch input", func(t *testing.T) {
		l.Input([]obj{{"1"}, {"2"}, {"3"}}, WithBatch())
		if l.q.Len() != 6 {
			t.FailNow()
		}
	})
	t.Run("input with highest prior", func(t *testing.T) {
		l.Input([]obj{{"1"}, {"2"}, {"3"}}, WithBatch(), WithHighestPriority())
		if l.q.Len() != 9 {
			t.FailNow()
		}
		if *l.highestPriority != 1 {
			t.FailNow()
		}
	})
}
