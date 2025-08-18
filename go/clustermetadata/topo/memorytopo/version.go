package memorytopo

import (
	"fmt"
)

// NodeVersion is the local topo.Version implementation
type NodeVersion uint64

func (v NodeVersion) String() string {
	return fmt.Sprintf("%v", uint64(v))
}
