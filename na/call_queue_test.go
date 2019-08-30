package na

import (
	"sync"
	"testing"
	"time"
)

func TestCallQueue(t *testing.T) {
	cq := GetCallQueue(func(data interface{}) (interface{}, error) {
		v := data.(int)
		time.Sleep(1 * time.Millisecond)
		return v * v, nil
	})

	var list []int
	for i := 0; i < 1000; i++ {
		vi, err := cq.Enqueue(i)
		if err != nil {
			t.Fatal(err)
		}
		v := vi.(int)
		list = append(list, v)
	}

	for i := 0; i < 1000; i++ {
		assertEqual(t, i*i, list[i], "")
	}
}

func TestCallQueue2(t *testing.T) {
	count := 0
	cq := GetCallQueue(func(data interface{}) (interface{}, error) {
		v := data.(int)
		time.Sleep(1 * time.Millisecond)
		count += v
		return nil, nil
	})

	n := 1000
	var wg sync.WaitGroup
	wg.Add(n)

	sum := 0
	for i := 0; i < n; i++ {
		sum += i
		go func(i int) {
			_, err := cq.Enqueue(i)
			if err != nil {
				t.Fatal(err)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	assertEqual(t, sum, count, "")
}

func TestCallQueueMap(t *testing.T) {
	cqm := GetCallQueueMap(func(data interface{}) (interface{}, error) {
		v := data.(int)
		time.Sleep(1 * time.Millisecond)
		return v * v, nil
	})

	ks, n := []string{"k1", "k2", "k3"}, 100
	var wg sync.WaitGroup
	wg.Add(len(ks))

	listMap := make(map[string][]int)
	var mu sync.Mutex
	for _, key := range ks {
		go func(key string) {
			for i := 0; i < n; i++ {
				vi, err := cqm.Enqueue(key, i)
				if err != nil {
					t.Fatal(err)
				}
				v := vi.(int)

				mu.Lock()
				listMap[key] = append(listMap[key], v)
				mu.Unlock()
			}
			wg.Done()
		}(key)
	}

	wg.Wait()
	for _, key := range ks {
		for i := 0; i < n; i++ {
			assertEqual(t, listMap[key][i], i*i, "")
		}
	}
}
