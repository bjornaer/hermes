package disk_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/bjornaer/hermes/internal/disk"
)

func printNodeElements(n *disk.DiskNode) {
	for i := 0; i < len(n.GetElements()); i++ {
		fmt.Println(n.GetElementAtIndex(i).Key, n.GetElementAtIndex(i).Value)
	}
}
func TestAddElement(t *testing.T) {
	blockService := initBlockService()
	elements := make([]*disk.Pairs, 3)
	elements[0] = disk.NewPair("hola", "amigos")
	elements[1] = disk.NewPair("foo", "bar")
	elements[2] = disk.NewPair("gooz", "bumps")
	n, err := disk.NewLeafNode(elements, blockService)
	if err != nil {
		t.Error(err)
	}
	addedElement := disk.NewPair("added", "please check")
	n.AddElement(addedElement)

	if !reflect.DeepEqual(n.GetElements(), []*disk.Pairs{addedElement, elements[0],
		elements[1], elements[2]}) {
		t.Error("Value not inserted at the correct position", n.GetElements())
	}

	n, err = disk.NewLeafNode([]*disk.Pairs{disk.NewPair("first", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	n.AddElement(disk.NewPair("second", "value"))
	if !reflect.DeepEqual(n.GetElements(), []*disk.Pairs{disk.NewPair("first", "value"),
		disk.NewPair("second", "value")}) {
		t.Error("Value not inserted at the correct position", n.GetElements())
	}

	n, err = disk.NewLeafNode([]*disk.Pairs{disk.NewPair("first", "value"),
		disk.NewPair("second", "value"), disk.NewPair("third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	n.AddElement(disk.NewPair("fourth", "value"))
	if !reflect.DeepEqual(n.GetElements(), []*disk.Pairs{disk.NewPair("first", "value"),
		disk.NewPair("fourth", "value"), disk.NewPair("second", "value"), disk.NewPair("third", "value")}) {
		t.Error("Value not inserted at the correct position", n.GetElements())
	}
}

func TestIsLeaf(t *testing.T) {
	blockService := initBlockService()
	child1, err := disk.NewLeafNode([]*disk.Pairs{disk.NewPair("first", "value"),
		disk.NewPair("second", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child2, err := disk.NewLeafNode([]*disk.Pairs{disk.NewPair("third", "value"),
		disk.NewPair("forth", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	n, err := disk.NewNodeWithChildren([]*disk.Pairs{disk.NewPair("fifth", "value"),
		disk.NewPair("sixth", "value")}, []uint64{child1.BlockID, child2.BlockID}, blockService)
	if err != nil {
		t.Error(err)
	}
	if n.IsLeaf() {
		t.Error("Should not return as leaf as it has children", n)
	}

	child1, err = disk.NewLeafNode(nil, blockService)
	if err != nil {
		t.Error(err)
	}
	child2, err = disk.NewLeafNode(nil, blockService)
	if err != nil {
		t.Error(err)
	}
	n, err = disk.NewNodeWithChildren([]*disk.Pairs{disk.NewPair("first", "value"),
		disk.NewPair("second", "value")}, nil, blockService)
	if err != nil {
		t.Error(err)
	}
	if !n.IsLeaf() {
		t.Error("Should return as leaf as it has no children", n)
	}

	n, err = disk.NewLeafNode([]*disk.Pairs{disk.NewPair("first", "value"),
		disk.NewPair("second", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	if !n.IsLeaf() {
		t.Error("Should return as leaf as it has no children", n)
	}
}

func TestHasOverFlown(t *testing.T) {
	blockService := initBlockService()
	elements := make([]*disk.Pairs, blockService.GetMaxLeafSize()+1)
	for i := 0; i < blockService.GetMaxLeafSize()+1; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		elements[i] = disk.NewPair(key, value)
	}
	n, err := disk.NewLeafNode(elements, blockService)
	if err != nil {
		t.Error(err)
	}
	if !n.HasOverFlown() {
		t.Error("Should return true as node has overflown", n)
	}

	n, err = disk.NewLeafNode([]*disk.Pairs{disk.NewPair("first", "value"), disk.NewPair("fourth", "value"),
		disk.NewPair("second", "value"), disk.NewPair("third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	if n.HasOverFlown() {
		t.Error("Should return false as node has 3 elements", n)
	}

	child1, err := disk.NewLeafNode([]*disk.Pairs{disk.NewPair("first", "value"),
		disk.NewPair("second", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child2, err := disk.NewLeafNode([]*disk.Pairs{disk.NewPair("third", "value"),
		disk.NewPair("fourth", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	n, err = disk.NewNodeWithChildren(elements, []uint64{child1.BlockID,
		child2.BlockID}, blockService)
	if err != nil {
		t.Error(err)
	}
	if !n.HasOverFlown() {
		t.Error("Should return true as node has overflown", n)
	}

}

func TestSplitLeafNode(t *testing.T) {
	blockService := initBlockService()
	n, err := disk.NewLeafNode([]*disk.Pairs{disk.NewPair("first", "value"),
		disk.NewPair("fourth", "value"), disk.NewPair("second", "value"), disk.NewPair("third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	poppedUpMiddleElement, leftChild, rightChild, err := n.SplitLeafNode()
	if err != nil {
		t.Error(err)
	}
	if poppedUpMiddleElement.Key != "second" {
		t.Error("Wrong middle Element popped up", poppedUpMiddleElement)
	}
	if leftChild.GetElementAtIndex(1).Key != "fourth" {
		t.Error("Wrong value at leftchild", leftChild)
	}
	if rightChild.GetElementAtIndex(0).Key != "third" {
		t.Error("Wrong value at rightchild ", rightChild)
	}
}

func TestSplitNonLeafNode(t *testing.T) {
	blockService := initBlockService()
	child1, err := disk.NewLeafNode([]*disk.Pairs{disk.NewPair("1first", "value"),
		disk.NewPair("1fourth", "value"), disk.NewPair("1second", "value"), disk.NewPair("1third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child2, err := disk.NewLeafNode([]*disk.Pairs{disk.NewPair("2first", "value"),
		disk.NewPair("2fourth", "value"), disk.NewPair("2second", "value"), disk.NewPair("2third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child3, err := disk.NewLeafNode([]*disk.Pairs{disk.NewPair("3first", "value"),
		disk.NewPair("3fourth", "value"), disk.NewPair("3second", "value"), disk.NewPair("3third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child4, err := disk.NewLeafNode([]*disk.Pairs{disk.NewPair("4first", "value"),
		disk.NewPair("4fourth", "value"), disk.NewPair("4second", "value"), disk.NewPair("4third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child5, err := disk.NewLeafNode([]*disk.Pairs{disk.NewPair("5first", "value"),
		disk.NewPair("5fourth", "value"), disk.NewPair("5second", "value"), disk.NewPair("5third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}

	n, err := disk.NewNodeWithChildren([]*disk.Pairs{disk.NewPair("nfirst", "value"),
		disk.NewPair("nfourth", "value"), disk.NewPair("nsecond", "value"), disk.NewPair("nthird", "value")},
		[]uint64{child1.BlockID, child2.BlockID, child3.BlockID,
			child4.BlockID, child5.BlockID}, blockService)
	if err != nil {
		t.Error(err)
	}
	poppedUpMiddleElement, leftChild, rightChild, err := n.SplitNonLeafNode()
	if err != nil {
		t.Error(err)
	}
	if poppedUpMiddleElement.Key != "nsecond" {
		t.Error("Wrong middle element, should be second", poppedUpMiddleElement)
	}
	childToBeTested, err := leftChild.GetChildAtIndex(2)
	if err != nil {
		t.Error(err)
	}
	if childToBeTested.GetElementAtIndex(2).Key != "3second" {
		t.Error("Element should be 3second", childToBeTested.GetElementAtIndex(2).Key)
	}
	childToBeTested, err = leftChild.GetChildAtIndex(1)
	if err != nil {
		t.Error(err)
	}
	if childToBeTested.GetElementAtIndex(3).Key != "2third" {
		t.Error("Element should be 2third", childToBeTested.GetElementAtIndex(3).Key)
	}

	childToBeTested, err = rightChild.GetChildAtIndex(1)
	if err != nil {
		t.Error(err)
	}
	if childToBeTested.GetElementAtIndex(3).Key != "5third" {
		t.Error("Element should be 5third", childToBeTested.GetElementAtIndex(3).Key)
	}
}

func TestAddPoppedupElement(t *testing.T) {
	blockService := initBlockService()
	child1OfParent, err := disk.NewLeafNode([]*disk.Pairs{disk.NewPair("1first", "value"),
		disk.NewPair("1fourth", "value"), disk.NewPair("1second", "value"), disk.NewPair("1third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child2OfParent, err := disk.NewLeafNode([]*disk.Pairs{disk.NewPair("2first", "value"),
		disk.NewPair("2fourth", "value"), disk.NewPair("2second", "value"), disk.NewPair("2third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	parentNode, err := disk.NewNodeWithChildren([]*disk.Pairs{disk.NewPair("parentfirst", "value")}, []uint64{child1OfParent.BlockID,
		child2OfParent.BlockID}, blockService)
	child3, err := disk.NewLeafNode([]*disk.Pairs{disk.NewPair("3first", "value"),
		disk.NewPair("3fourth", "value"), disk.NewPair("3second", "value"), disk.NewPair("3third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child4, err := disk.NewLeafNode([]*disk.Pairs{disk.NewPair("4first", "value"),
		disk.NewPair("4fourth", "value"), disk.NewPair("4second", "value"), disk.NewPair("4third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	parentNode.AddPoppedUpElementIntoCurrentNodeAndUpdateWithNewChildren(disk.NewPair("popfirst", "value"), child3, child4)

	child, err := parentNode.GetChildAtIndex(0)
	if err != nil {
		t.Error(err)
	}
	if child.GetElementAtIndex(0).Key != "1first" {
		t.Error("Child not inserted at the correct position", child.GetElements())
	}

	child, err = parentNode.GetChildAtIndex(2)
	if err != nil {
		t.Error(err)
	}
	if child.GetElementAtIndex(0).Key != "4first" {
		printNodeElements(child)
		t.Error("Child not inserted at the correct position", child.GetElements())
	}

}
