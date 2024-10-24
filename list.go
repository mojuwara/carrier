package main

type Node struct {
	Msg  *Message
	Next *Node
}

type List struct {
	Head *Node
	Tail *Node
}

// Add elements to the end of the List
func (l *List) Push(msg *Message) {
	n := &Node{Msg: msg}
	if l.Head == nil {
		l.Head = n
		l.Tail = n
		return
	}

	l.Tail.Next = n
	l.Tail = l.Tail.Next
}

// Removes element at the front of the List
func (l *List) Pop() {
	if l.Head == l.Tail {
		l.Head = nil
		l.Tail = nil
		return
	}

	l.Head = l.Head.Next
}

// Returns the element at the front of the list, possibly nil
func (l *List) Empty() bool {
	return l.Head == nil
}
