package common

// hash function
// TODO: try crc32 or something else?
func Hash_fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

type Interval struct {
	start int
	end   int
}

type Node struct {
	Metric    string `json:"text"`
	Is_leaf   bool   `json:"isLeaf,omitempty"`
	intervals []Interval
}

func NewNode(path string, is_leaf bool) *Node {
	return &Node{
		Metric:  path,
		Is_leaf: is_leaf,
	}
}

type NodeList []*Node

func (nl NodeList) Len() int {
	return len(nl)
}

func (nl NodeList) Swap(i, j int) {
	nl[i], nl[j] = nl[j], nl[i]
}

func (nl NodeList) Less(i, j int) bool {
	return nl[i].Metric < nl[j].Metric
}
