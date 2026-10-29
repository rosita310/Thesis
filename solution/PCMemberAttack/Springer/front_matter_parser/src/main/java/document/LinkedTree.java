package document;

import java.util.ArrayList;
import java.util.List;

/**
 * Based on Data structures and algorithms in java 6th edition, p.297-300
 *
 * @param <E>
 */
public class LinkedTree<E> extends AbstractTree<E> {
    protected static class Node<E> implements Position<E> {

        private E element;
        private Node<E> parent;
        private List<Node<E>> children;

        public Node(E e, Node<E> parent) {
            element = e;
            this.parent = parent;
            children = new ArrayList<>();
        }

        @Override
        public E getElement() throws IllegalStateException {
            return element;
        }

        public Node<E> getParent() {
            return parent;
        }

        public List<? extends Position<E>> getChildren() {
            return children;
        }

        public void setElement(E e) {
            element = e;
        }

        public void setParent(Node<E> parentNode) {
            parent = parentNode;
        }

        public void addChild(Node<E> childNode) {
            children.add(childNode);
        }

        public int numberOfChildren() {
            return children.size();
        }
    }

    /**
     * Factory function to create a new Node storing E e.
     * @param e
     * @param parent
     * @return
     */
    protected Node<E> createNode(E e, Node<E> parent) {
        return new Node<E>(e, parent);
    }

    protected Node<E> root = null;
    protected int size = 0;

    /**
     * Constructor
     */
    public LinkedTree() {

    }

    protected Node<E> validate(Position<E> p) throws IllegalArgumentException {
        if (!(p instanceof Node)) {
            throw new IllegalArgumentException("Not valid position type");
        }
        Node<E> node = (Node<E>)p;
        if (node.getParent() == node) {
            throw new IllegalArgumentException("p is no longer in the tree");
        }
        return node;
    }

    @Override
    public Position<E> root() {
        return root;
    }

    @Override
    public Position<E> parent(Position<E> p) throws IllegalStateException {
        Node<E> node = validate(p);
        return node.getParent();
    }

    @Override
    public List<Position<E>> children(Position<E> p) throws IllegalStateException {
        Node<E> node = validate(p);
        return (List<Position<E>>) node.getChildren();
    }

    @Override
    public int numChildren(Position<E> p) throws IllegalStateException {
        Node<E> node = validate(p);
        return node.numberOfChildren();
    }

    @Override
    public int size() {
        return size;
    }

    /**
     * Places E e at the root of an empty tree.
     * @param e
     * @return new Position
     * @throws IllegalStateException
     */
    public Position<E> addRoot(E e) throws IllegalStateException {
        if (!isEmpty()) {
            throw new IllegalStateException("Tree is not empty");
        }
        root = createNode(e, null);
        size = 1;
        return root;
    }

    /**
     * Creates a new child of Position p storing E e
     * @param p
     * @param e
     * @return Position of new child
     * @throws IllegalArgumentException
     */
    public Position<E> addChild(Position<E> p, E e) throws IllegalArgumentException {
        Node<E> parent = validate(p);
        Node<E> child = createNode(e, parent);
        parent.addChild(child);
        size++;
        return child;
    }

    /**
     * Replaced the element at Position p with E e
     * @param p
     * @param e
     * @return replaced element
     * @throws IllegalArgumentException
     */
    public E set(Position<E> p, E e) throws IllegalArgumentException {
        Node<E> node = validate(p);
        E temp = node.getElement();
        node.setElement(e);
        return temp;
    }


}
