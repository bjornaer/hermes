package disk

import (
	"fmt"
	"time"
)

// node - Interface for node
type node interface {
	InsertPair(value *Pairs, bt *Btree) error
	GetValue(key string) (string, time.Time, error)
	PrintTree(level int)
	Size() int
}

// DiskBlock -- Make sure that it is accomodated in blockSize = 4096
type DiskBlock struct {
	Id                  uint64   // 4096 - 8 = 4088
	CurrentLeafSize     uint64   // 4088 - 8 = 4080
	currentChildrenSize uint64   // 4080 - 8 = 4072
	ChildrenBlockIds    []uint64 // 352 - (8 * 30) =  112
	DataSet             []*Pairs // 4072 - (124 * 30) = 352
}

func (b *DiskBlock) SetData(data []*Pairs) {
	b.DataSet = data
	b.CurrentLeafSize = uint64(len(data))
}

func (b *DiskBlock) SetChildren(childrenBlockIds []uint64) {
	b.ChildrenBlockIds = childrenBlockIds
	b.currentChildrenSize = uint64(len(childrenBlockIds))
}

// DiskNode - In memory node implementation
type DiskNode struct {
	Keys             []*Pairs
	ChildrenBlockIDs []uint64
	BlockID          uint64
	BlockService     *BlockService
}

/**
* Insertion Algorithm
1. It will begin from root and value will always be inserted into a leaf node
2. Insert Function Begin
3. If current node is leaf node, then return pick current node, Current Node Insertion Algorithm
    1. This section gives 3 outputs, 1 middle element and 2 child nodes or null,null,null
    2. Insert into the current node
    3. If its full, then sort it and make two new child nodes without the middle node ( NODE CREATION WILL TAKE PLACE HERE)
    4. take out the middle element along with the two child nodes,  Leaf Splitting no children Algorithm:
        1. Pick middle element by using length of array/2, lets say its index i
        2. Club all elements from 0 to i-1, and i+1 to len(array) and create new seperate nodes by inserting these 2 arrays into the respective keys[] of respective nodes
        3. Since the current node is a leaf node, we do not need to worry about its children and we can leave them to be null for both
        4. return middle,leftNode,rightNode
    5. If its not full, then return null,null,null
4. If this is not a leaf node, then find out the proper child node, Child Node Searching Algorithm:
    1. Input : Value to be inserted, the current Node. Output : Pointer to the childnode
    2. Since the list of values/elements is sorted, perform a binary or linear search to find the first element greater than the value to be inserted, if such an element is found, return pointer at position i, else return last pointer ( ie. the last pointer)
5. After getting the pointer to that element, call insert function Step 2 on that node RECURSIVELY ONLY HERE
6. If we get output from child node insert function Step 2, then this means that we have to insert the middle element received and accomodate the 2 pointers in the current node as well  discarding the old pointer ( NODE DESTRUCTION WILL ONLY TAKE PLACE HERE )
    1. If we got null as output then do nothing, else
    2. Insert into current Node, Popped up element and two child pointers insertion algorithm, Popped Up Joining Algorithm:
        1. Insert element and sort the array
        2. Now we need to discard 1 child pointer and insert 2 child pointers, Child Pointer Manipulation Algorithm :
        3. Find index of inserted element in array, lets say that it is i
        4. Now in the child pointer array, insert the left and right pointers at ith and i+1 th index
    3. If its full, sort it and make two new child nodes, Leaf Splitting with children Algorithm:
        1. Pick middle element by using length of array/2, lets say its index i (Same as 3.4.1)
        2. Club all elements from 0 to i-1, and i+1 to len(lkeys array) and create new seperate nodes by inserting these 2 arrays into the respective keys[] of respective nodes (Same as 3.4.2)
        3. For children[], split the current node's children array into 2 parts, part1 will be from 0 to i, and part 2 will be from i+1 to len(children array), and insert them into leftNode children, and rightNode children
        4. If current node is not the root node return middle,leftNode,rightNode
        5. else if current node == rootNode, Root Node Splitting Algorithm:
            1. Create a new node with elements array as keys[0] = middle
            2. children[0]=leftNode and children[1]=rightNode
            3. Set btree.root=new node
            4. return null,null,null

*/

func (n *DiskNode) IsLeaf() bool {
	return len(n.ChildrenBlockIDs) == 0
}

// argh Size always returns the size of the full data set, not the current node, because I'm a lazy mf
func (n *DiskNode) Size() int {
	root, err := n.BlockService.GetRootBlock()
	if err != nil {
		return 0
	}
	return len(root.DataSet)
}

// PrintTree - Traverse and print the entire tree
func (n *DiskNode) PrintTree(level int) {
	currentLevel := level
	if level == 0 {
		currentLevel = 1
	}

	n.printNode()
	for i := 0; i < len(n.ChildrenBlockIDs); i++ {
		fmt.Println("Printing ", i+1, " th child of level : ", currentLevel)
		childNode, err := n.GetChildAtIndex(i)
		if err != nil {
			panic(err)
		}
		childNode.PrintTree(currentLevel + 1)
	}
}

/**
* Do a linear search and insert the element
 */
func (n *DiskNode) AddElement(element *Pairs) int {
	elements := n.GetElements()
	indexForInsertion := 0
	elementInsertedInBetween := false
	for i := 0; i < len(elements); i++ {
		if elements[i].Key >= element.Key {
			// We have found the right place to insert the element

			indexForInsertion = i
			elements = append(elements, nil)
			copy(elements[indexForInsertion+1:], elements[indexForInsertion:])
			elements[indexForInsertion] = element
			n.setElements(elements)
			elementInsertedInBetween = true
			break
		}
	}
	if !elementInsertedInBetween {
		// If we are here, it means we need to insert the element at the rightmost position
		n.setElements(append(elements, element))
		indexForInsertion = len(n.GetElements()) - 1
	}

	return indexForInsertion
}

func (n *DiskNode) HasOverFlown() bool {
	return len(n.GetElements()) > n.BlockService.GetMaxLeafSize()
}

func (n *DiskNode) GetElements() []*Pairs {
	return n.Keys
}

func (n *DiskNode) setElements(newElements []*Pairs) {
	n.Keys = newElements
}

func (n *DiskNode) GetElementAtIndex(index int) *Pairs {
	return n.Keys[index]
}

func (n *DiskNode) GetChildAtIndex(index int) (*DiskNode, error) {
	return n.BlockService.getNodeAtBlockID(n.ChildrenBlockIDs[index])
}

func (n *DiskNode) shiftRemainingChildrenToRight(index int) {
	if len(n.ChildrenBlockIDs) < index+1 {
		// This means index is the last element, hence no need to shift
		return
	}
	n.ChildrenBlockIDs = append(n.ChildrenBlockIDs, 0)
	copy(n.ChildrenBlockIDs[index+1:], n.ChildrenBlockIDs[index:])
	n.ChildrenBlockIDs[index] = 0
}
func (n *DiskNode) setChildAtIndex(index int, childNode *DiskNode) {
	if len(n.ChildrenBlockIDs) < index+1 {
		n.ChildrenBlockIDs = append(n.ChildrenBlockIDs, 0)
	}
	n.ChildrenBlockIDs[index] = childNode.BlockID
}

func (n *DiskNode) getLastChildNode() (*DiskNode, error) {
	return n.GetChildAtIndex(len(n.ChildrenBlockIDs) - 1)
}

func (n *DiskNode) getChildNodes() ([]*DiskNode, error) {
	childNodes := make([]*DiskNode, len(n.ChildrenBlockIDs))
	for index := range n.ChildrenBlockIDs {
		childNode, err := n.GetChildAtIndex(index)
		if err != nil {
			return nil, err
		}
		childNodes[index] = childNode
	}
	return childNodes, nil
}

func (n *DiskNode) getChildBlockIDs() []uint64 {
	return n.ChildrenBlockIDs
}

func (n *DiskNode) printNode() {
	fmt.Println("Printing Node")
	fmt.Println("--------------")
	for i := 0; i < len(n.GetElements()); i++ {
		fmt.Println(n.GetElementAtIndex(i))
	}
	fmt.Println("**********************")
}

// SplitLeafNode - Split leaf node
func (n *DiskNode) SplitLeafNode() (*Pairs, *DiskNode, *DiskNode, error) {
	/**
		LEAF SPLITTING WITHOUT CHILDREN ALGORITHM
				If its full, then  make two new child nodes without the middle node ( NODE CREATION WILL TAKE PLACE HERE)
	    		Take out the middle element along with the two child nodes,  Leaf Splitting no children Algorithm:
	        	1. Pick middle element by using length of array/2, lets say its index i
	        	2. Club all elements from 0 to i-1, and i+1 to len(array) and create new seperate nodes by inserting these 2 arrays into the respective keys[] of respective nodes
	        	3. Since the current node is a leaf node, we do not need to worry about its children and we can leave them to be null for both
	        	4. return middle,leftNode,rightNode
	*/
	elements := n.GetElements()
	midIndex := len(elements) / 2
	middle := elements[midIndex]

	// Now lets split elements array into 2 as we are splitting this node
	elements1 := elements[0:midIndex]
	elements2 := elements[midIndex+1:]

	// Now lets construct new Nodes from these 2 element arrays
	leftNode, err := NewLeafNode(elements1, n.BlockService)
	if err != nil {
		return nil, nil, nil, err
	}
	rightNode, err := NewLeafNode(elements2, n.BlockService)
	if err != nil {
		return nil, nil, nil, err
	}
	return middle, leftNode, rightNode, nil
}

//SplitNonLeafNode - Split non leaf node
func (n *DiskNode) SplitNonLeafNode() (*Pairs, *DiskNode, *DiskNode, error) {
	/**
		NON-LEAF NODE SPLITTING ALGORITHM WITH CHILDREN MANIPULATION
		If its full, sort it and make two new child nodes, Leaf Splitting with children Algorithm:
	        1. Pick middle element by using length of array/2, lets say its index i (Same as 3.4.1)
			2. Club all elements from 0 to i-1, and i+1 to len(lkeys array) and create new seperate nodes
			   by inserting these 2 arrays into the respective keys[] of respective nodes (Same as 3.4.2)
			3. For children[], split the current node's children array into 2 parts, part1 will be
			   from 0 to i, and part 2 will be from i+1 to len(children array), and insert them into
			   leftNode children, and rightNode children

		NOTE : NODE CREATION WILL TAKE PLACE HERE
	*/
	elements := n.GetElements()
	midIndex := len(elements) / 2
	middle := elements[midIndex]

	// Now lets split elements array into 2 as we are splitting this node
	elements1 := elements[0:midIndex]
	elements2 := elements[midIndex+1:]

	// Lets split the children
	children := n.ChildrenBlockIDs

	children1 := children[0 : midIndex+1]
	children2 := children[midIndex+1:]

	// Now lets construct new Nodes from these 2 element arrays
	leftNode, err := NewNodeWithChildren(elements1, children1, n.BlockService)
	if err != nil {
		return nil, nil, nil, err
	}
	rightNode, err := NewNodeWithChildren(elements2, children2, n.BlockService)
	if err != nil {
		return nil, nil, nil, err
	}
	return middle, leftNode, rightNode, nil
}

// AddPoppedUpElementIntoCurrentNodeAndUpdateWithNewChildren - Insert element received as a reaction
// from insert operation at child nodes
func (n *DiskNode) AddPoppedUpElementIntoCurrentNodeAndUpdateWithNewChildren(element *Pairs, leftNode *DiskNode, rightNode *DiskNode) {
	/**
		POPPED UP JOINING ALGORITHM
			Insert into current Node, Popped up element and two child pointers insertion algorithm, Popped Up Joining Algorithm:
	        1. Insert element and sort the array
	        2. Now we need to discard 1 child pointer and insert 2 child pointers, Child Pointer Manipulation Algorithm :
	        3. Find index of inserted element in array, lets say that it is i
	        4. Now in the child pointer array, insert the left and right pointers at ith and i+1 th index
	*/

	//CHILD POINTER MANIPULATION ALGORITHM
	insertionIndex := n.AddElement(element)
	n.setChildAtIndex(insertionIndex, leftNode)
	//Shift remaining elements to the right and add this
	n.shiftRemainingChildrenToRight(insertionIndex + 1)
	n.setChildAtIndex(insertionIndex+1, rightNode)
}

// NewLeafNode - Create a new leaf node without children
func NewLeafNode(elements []*Pairs, bs *BlockService) (*DiskNode, error) {
	node := &DiskNode{Keys: elements, BlockService: bs}
	//persist the node to disk
	err := bs.saveNewNodeToDisk(node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// NewNodeWithChildren - Create a non leaf node with children
func NewNodeWithChildren(elements []*Pairs, childrenBlockIDs []uint64, bs *BlockService) (*DiskNode, error) {
	node := &DiskNode{Keys: elements, ChildrenBlockIDs: childrenBlockIDs, BlockService: bs}
	//persist this node to disk
	err := bs.saveNewNodeToDisk(node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// newRootNodeWithSingleElementAndTwoChildren - Create a new root node
func newRootNodeWithSingleElementAndTwoChildren(element *Pairs, leftChildBlockID uint64,
	rightChildBlockID uint64, bs *BlockService) (*DiskNode, error) {
	elements := []*Pairs{element}
	childrenBlockIDs := []uint64{leftChildBlockID, rightChildBlockID}
	node := &DiskNode{Keys: elements, ChildrenBlockIDs: childrenBlockIDs, BlockService: bs}
	//persist this node to disk
	err := bs.updateRootNode(node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// getChildNodeForElement - Get Correct Traversal path for insertion
func (n *DiskNode) getChildNodeForElement(key string) (*DiskNode, error) {
	/** CHILD NODE SEARCHING ALGORITHM
		If this is not a leaf node, then find out the proper child node, Child Node Searching Algorithm:
	    1. Input : Value to be inserted, the current Node. Output : Pointer to the childnode
		2. Since the list of values/elements is sorted, perform a binary or linear search to find the
		   first element greater than the value to be inserted, if such an element is found, return pointer at position i, else return last pointer ( ie. the last pointer)
	*/

	for i := 0; i < len(n.GetElements()); i++ {
		if key < n.GetElementAtIndex(i).Key {
			return n.GetChildAtIndex(i)
		}
	}
	// This means that no element is found with value greater than the element to be inserted
	// so we need to return the last child node
	return n.getLastChildNode()
}

func (n *DiskNode) insert(value *Pairs, bt *Btree) (*Pairs, *DiskNode, *DiskNode, error) {
	if n.IsLeaf() {
		n.AddElement(value)
		if !n.HasOverFlown() {
			// So lets store this updated node on disk
			err := n.BlockService.updateNodeToDisk(n)
			if err != nil {
				return nil, nil, nil, err
			}
			return nil, nil, nil, nil
		}
		if bt.IsRootNode(n) {
			poppedMiddleElement, leftNode, rightNode, err := n.SplitLeafNode()
			if err != nil {
				return nil, nil, nil, err
			}
			//NOTE : NODE CREATION WILL TAKE PLACE HERE
			newRootNode, err := newRootNodeWithSingleElementAndTwoChildren(poppedMiddleElement,
				leftNode.BlockID, rightNode.BlockID, n.BlockService)
			if err != nil {
				return nil, nil, nil, err
			}
			bt.setRootNode(newRootNode)
			return nil, nil, nil, nil

		}
		// Split the node and return to parent function with pooped up element and left,right nodes
		return n.SplitLeafNode()

	}
	// Get the child Node for insertion
	childNodeToBeInserted, err := n.getChildNodeForElement(value.Key)
	if err != nil {
		return nil, nil, nil, err
	}
	poppedMiddleElement, leftNode, rightNode, err := childNodeToBeInserted.insert(value, bt)
	if err != nil {
		return nil, nil, nil, err
	}
	if poppedMiddleElement == nil {
		// this means element has been inserted into the child and hence we do nothing
		return poppedMiddleElement, leftNode, rightNode, nil
	}
	// Insert popped up element into current node along with updating the child pointers
	// with new left and right nodes returned
	n.AddPoppedUpElementIntoCurrentNodeAndUpdateWithNewChildren(poppedMiddleElement, leftNode, rightNode)

	if !n.HasOverFlown() {
		// this means that element has been easily inserted into current parent Node
		// without overflowing
		err := n.BlockService.updateNodeToDisk(n)
		if err != nil {
			return nil, nil, nil, err
		}
		// So lets store this updated node on disk
		return nil, nil, nil, nil
	}
	// this means that the current parent node has overflown, we need to split this up
	// and move the popped up element upwards if this is not the root
	poppedMiddleElement, leftNode, rightNode, err = n.SplitNonLeafNode()
	if err != nil {
		return nil, nil, nil, err
	}
	/**
		If current node is not the root node return middle,leftNode,rightNode
	    else if current node == rootNode, Root Node Splitting Algorithm:
	            1. Create a new node with elements array as keys[0] = middle
	            2. children[0]=leftNode and children[1]=rightNode
	            3. Set btree.root=new node
	            4. return null,null,null
	*/

	if !bt.IsRootNode(n) {
		return poppedMiddleElement, leftNode, rightNode, nil
	}
	newRootNode, err := newRootNodeWithSingleElementAndTwoChildren(poppedMiddleElement,
		leftNode.BlockID, rightNode.BlockID, n.BlockService)
	if err != nil {
		return nil, nil, nil, err
	}

	//@Todo: Update the metadata somewhere so that we can read this new root node
	//next time
	bt.setRootNode(newRootNode)
	return nil, nil, nil, nil
}

func (n *DiskNode) searchElementInNode(key string) (string, time.Time, bool) {
	for i := 0; i < len(n.GetElements()); i++ {
		e := n.GetElementAtIndex(i)
		if e.Key == key {
			return e.Value, e.Timestamp, true
		}
	}
	return "", time.Time{}, false
}
func (n *DiskNode) search(key string) (string, time.Time, error) {
	/*
		Algo:
		1. Find key in current node, if this is leaf node, then return as not found
		2. Then find the appropriate child node
		3. goto step 1
	*/
	value, addedAt, foundInCurrentNode := n.searchElementInNode(key)

	if foundInCurrentNode {
		return value, addedAt, nil
	}

	if n.IsLeaf() {
		return "", time.Time{}, nil
	}

	node, err := n.getChildNodeForElement(key)
	if err != nil {
		return "", time.Time{}, err
	}
	return node.search(key)
}

// Insert - Insert value into Node
func (n *DiskNode) InsertPair(value *Pairs, bt *Btree) error {
	_, _, _, err := n.insert(value, bt)
	if err != nil {
		return err
	}
	return nil
}

func (n *DiskNode) GetValue(key string) (string, time.Time, error) {
	return n.search(key)
}
