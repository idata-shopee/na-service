package na

import (
	"fmt"
	"testing"
)

func assertEqual(t *testing.T, expect interface{}, actual interface{}, message string) {
	if expect == actual {
		return
	}
	if len(message) == 0 {
		message = fmt.Sprintf("expect %v !=  actual %v", expect, actual)
	}
	t.Fatal(message)
}

func TestBase(t *testing.T) {
	wlb := GetWorkerLB()

	w1 := Worker{Id: "0", ServiceType: "a"}
	w2 := Worker{Id: "1", ServiceType: "a"}
	wlb.AddWorker(w1)
	wlb.AddWorker(w2)

	for i := 0; i < 100; i++ {
		_, ok := wlb.PickUpWorker("a")
		assertEqual(t, ok, true, "")
	}

	for i := 0; i < 100; i++ {
		_, ok := wlb.PickUpWorker("b")
		assertEqual(t, ok, false, "")
	}

	wlb.RemoveWorker(w1)
	for i := 0; i < 100; i++ {
		_, ok := wlb.PickUpWorker("a")
		assertEqual(t, ok, true, "")
	}
	wlb.RemoveWorker(w2)
	for i := 0; i < 100; i++ {
		_, ok := wlb.PickUpWorker("a")
		assertEqual(t, ok, false, "")
	}
}

func TestRemoveFalsy(t *testing.T) {
	wlb := GetWorkerLB()

	w1 := Worker{Id: "0", ServiceType: "a"}
	w2 := Worker{Id: "1", ServiceType: "a"}
	assertEqual(t, wlb.RemoveWorker(w1), false, "")
	wlb.AddWorker(w2)
	assertEqual(t, wlb.RemoveWorker(w2), true, "")
	wlb.AddWorker(w1)
	assertEqual(t, wlb.RemoveWorker(w1), true, "")
}
