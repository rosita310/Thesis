package document;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * An abstract base class providing some functionality of the Tree interface
 *
 * Source: Data structures and algorithms in java 6th edition p.285-287, p.310-311
 *
 * @param <E>
 */
public abstract class AbstractTree<E> implements Tree<E> {

    private class ElementIterator implements Iterator<E> {

        Iterator<Position<E>> posIterator = positions().iterator();

        @Override
        public boolean hasNext() {
            return posIterator.hasNext();
        }

        @Override
        public E next() {
            return posIterator.next().getElement();
        }

        public void remove() {
            posIterator.remove();
        }
    }

    public boolean isInternal(Position<E> p) {
        return numChildren(p) > 0;
    }

    public boolean isExternal(Position<E> p) {
        return numChildren(p) == 0;
    }

    public boolean isRoot(Position<E> p) {
        return p == root();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public Iterator<E> iterator() {
        return new ElementIterator();
    }

    public Iterable<Position<E>> positions() {
        return preorder();
    }


    /**
     * Adds positions of the subtree rooted at Position p to the given snapshot.
     * @param p
     * @param snapshot
     */
    private void preorderSubtree(Position<E> p, List<Position<E>> snapshot) {
        snapshot.add(p);
        for(Position<E> c : children(p)) {
            preorderSubtree(c, snapshot);
        }
    }

    /**
     * @return an iterable collection of positions of the tree, reported in preorder
     */
    public Iterable<Position<E>> preorder() {
        if(!isEmpty()) {
            return preorder(root());
        }
        return new ArrayList<>();
    }

    public Iterable<Position<E>> preorder(Position<E> p) {
        List<Position<E>> snapshot = new ArrayList<>();
        preorderSubtree(p, snapshot);
        return snapshot;
    }

    /**
     * @param p
     * @return the number of levels separating Position p from the root.
     */
    public int depth(Position<E> p) {
        if (isRoot(p))
            return 0;
        else
            return 1 + depth(parent(p));
    }

    /**
     * @param p
     * @return the height of the subtree rooted at Position p.
     */
    public int height(Position<E> p) {
        int h = 0;
        for (Position<E> c : children(p)) {
            h = Math.max(h, 1 + height(c));
        }
        return h;
    }

    public String preOrderRepresentation() {
        return preOrderRepresentation(root(), 0);
    }

    protected String preOrderRepresentation(Position<E> p, int indent) {
        StringBuilder sb = new StringBuilder();
        sb.append("- ".repeat(indent) + p.getElement() + System.lineSeparator());
        for (Position<E> c : children(p)) {
            sb.append((preOrderRepresentation(c, indent + 1)));
        }
        return sb.toString();
    }

}
