package pqueue

type QItem struct {
	Value    string
	Index    int
	Priority float64
}

type PQueue []*QItem

func (pq PQueue) Len() int { return len(pq) }

func (pq PQueue) Less(i, j int) bool { return pq[i].Priority < pq[j].Priority }

func (pq PQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *PQueue) Push(x interface{}) {
	n := len(*pq)
	item, _ := x.(*QItem)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *PQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.Index = -1
	*pq = old[0 : n-1]

	return item
}
