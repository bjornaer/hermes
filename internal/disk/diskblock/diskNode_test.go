package diskblock_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/bjornaer/hermes/internal/disk/diskblock"
	"github.com/bjornaer/hermes/internal/disk/pair"
)

func printNodeElements(n *diskblock.DiskNode) {
	for i := 0; i < len(n.GetElements()); i++ {
		fmt.Println(n.GetElementAtIndex(i).Key, n.GetElementAtIndex(i).Value)
	}
}
func TestAddElement(t *testing.T) {
	blockService := initBlockService()
	elements := make([]*pair.Pairs, 3)
	elements[0] = pair.NewPair("hola", "amigos")
	elements[1] = pair.NewPair("foo", "bar")
	elements[2] = pair.NewPair("gooz", "bumps")
	n, err := diskblock.NewLeafNode(elements, blockService)
	if err != nil {
		t.Error(err)
	}
	addedElement := pair.NewPair("added", "please check")
	n.AddElement(addedElement)

	if !reflect.DeepEqual(n.GetElements(), []*pair.Pairs{addedElement, elements[0],
		elements[1], elements[2]}) {
		t.Error("Value not inserted at the correct position", n.GetElements())
	}
}

func TestAddElementInOrder(t *testing.T) {
	blockService := initBlockService()
	first := pair.NewPair("first", "value")
	second := pair.NewPair("second", "value")
	n, err := diskblock.NewLeafNode([]*pair.Pairs{first}, blockService)
	if err != nil {
		t.Error(err)
	}
	n.AddElement(second)
	if !reflect.DeepEqual(n.GetElements(), []*pair.Pairs{first, second}) {
		t.Error("Value not inserted at the correct position", n.GetElements())
	}

	third := pair.NewPair("third", "value")
	fourth := pair.NewPair("fourth", "value")
	n, err = diskblock.NewLeafNode([]*pair.Pairs{first, second, third}, blockService)
	if err != nil {
		t.Error(err)
	}
	n.AddElement(fourth)
	if !reflect.DeepEqual(n.GetElements(), []*pair.Pairs{first,
		fourth, second, third}) {
		t.Error("Value not inserted at the correct position", n.GetElements())
	}

}

func TestIsLeaf(t *testing.T) {
	blockService := initBlockService()
	child1, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("first", "value"),
		pair.NewPair("second", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child2, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("third", "value"),
		pair.NewPair("forth", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	n, err := diskblock.NewNodeWithChildren([]*pair.Pairs{pair.NewPair("fifth", "value"),
		pair.NewPair("sixth", "value")}, []uint64{child1.BlockID, child2.BlockID}, blockService)
	if err != nil {
		t.Error(err)
	}
	if n.IsLeaf() {
		t.Error("Should not return as leaf as it has children", n)
	}

	child1, err = diskblock.NewLeafNode(nil, blockService)
	if err != nil {
		t.Error(err)
	}
	child2, err = diskblock.NewLeafNode(nil, blockService)
	if err != nil {
		t.Error(err)
	}
	n, err = diskblock.NewNodeWithChildren([]*pair.Pairs{pair.NewPair("first", "value"),
		pair.NewPair("second", "value")}, nil, blockService)
	if err != nil {
		t.Error(err)
	}
	if !n.IsLeaf() {
		t.Error("Should return as leaf as it has no children", n)
	}

	n, err = diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("first", "value"),
		pair.NewPair("second", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	if !n.IsLeaf() {
		t.Error("Should return as leaf as it has no children", n)
	}
}

func TestHasOverFlown(t *testing.T) {
	blockService := initBlockService()
	elements := make([]*pair.Pairs, blockService.GetMaxLeafSize()+1)
	for i := 0; i < blockService.GetMaxLeafSize()+1; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		elements[i] = pair.NewPair(key, value)
	}
	n, err := diskblock.NewLeafNode(elements, blockService)
	if err != nil {
		t.Error(err)
	}
	if !n.HasOverFlown() {
		t.Error("Should return true as node has overflown", n)
	}

	n, err = diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("first", "value"), pair.NewPair("fourth", "value"),
		pair.NewPair("second", "value"), pair.NewPair("third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	if n.HasOverFlown() {
		t.Error("Should return false as node has 3 elements", n)
	}

	child1, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("first", "value"),
		pair.NewPair("second", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child2, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("third", "value"),
		pair.NewPair("fourth", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	n, err = diskblock.NewNodeWithChildren(elements, []uint64{child1.BlockID,
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
	n, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("first", "value"),
		pair.NewPair("fourth", "value"), pair.NewPair("second", "value"), pair.NewPair("third", "value")}, blockService)
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
	child1, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("1first", "value"),
		pair.NewPair("1fourth", "value"), pair.NewPair("1second", "value"), pair.NewPair("1third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child2, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("2first", "value"),
		pair.NewPair("2fourth", "value"), pair.NewPair("2second", "value"), pair.NewPair("2third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child3, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("3first", "value"),
		pair.NewPair("3fourth", "value"), pair.NewPair("3second", "value"), pair.NewPair("3third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child4, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("4first", "value"),
		pair.NewPair("4fourth", "value"), pair.NewPair("4second", "value"), pair.NewPair("4third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child5, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("5first", "value"),
		pair.NewPair("5fourth", "value"), pair.NewPair("5second", "value"), pair.NewPair("5third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}

	n, err := diskblock.NewNodeWithChildren([]*pair.Pairs{pair.NewPair("nfirst", "value"),
		pair.NewPair("nfourth", "value"), pair.NewPair("nsecond", "value"), pair.NewPair("nthird", "value")},
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
	child1OfParent, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("1first", "value"),
		pair.NewPair("1fourth", "value"), pair.NewPair("1second", "value"), pair.NewPair("1third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child2OfParent, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("2first", "value"),
		pair.NewPair("2fourth", "value"), pair.NewPair("2second", "value"), pair.NewPair("2third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	parentNode, err := diskblock.NewNodeWithChildren([]*pair.Pairs{pair.NewPair("parentfirst", "value")}, []uint64{child1OfParent.BlockID,
		child2OfParent.BlockID}, blockService)
	child3, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("3first", "value"),
		pair.NewPair("3fourth", "value"), pair.NewPair("3second", "value"), pair.NewPair("3third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	child4, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("4first", "value"),
		pair.NewPair("4fourth", "value"), pair.NewPair("4second", "value"), pair.NewPair("4third", "value")}, blockService)
	if err != nil {
		t.Error(err)
	}
	parentNode.AddPoppedUpElementIntoCurrentNodeAndUpdateWithNewChildren(pair.NewPair("popfirst", "value"), child3, child4)

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
