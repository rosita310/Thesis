package document;

import java.util.Iterator;

/**
 * And interface for a tree where nodes can have an arbitrary number of children.
 * Source: Data structures and algorithms in java 6th edition p.284
 *
 * @param <E>
 */
public interface Tree<E> extends Iterable<E> {
    Position<E> root();
    Position<E> parent(Position<E> p) throws IllegalStateException;
    Iterable<Position<E>> children(Position<E> p) throws IllegalStateException;
    int numChildren(Position<E> p) throws IllegalStateException;
    boolean isInternal(Position<E> p) throws IllegalStateException;
    boolean isExternal(Position<E> p) throws IllegalStateException;

    /**
     * @param p
     * @return the position of the root of the tree
     * @throws IllegalStateException
     */
    boolean isRoot(Position<E> p) throws IllegalStateException;
    int size();
    boolean isEmpty();

    /**
     * @return an iterator of the elements stored in the tree
     */
    Iterator<E> iterator();
    Iterable<Position<E>> positions();
    Iterable<Position<E>> preorder(Position<E> p);

    /**
     *
     * @return Textual representatino of the tree.
     */
    String preOrderRepresentation();
}
