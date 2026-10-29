package document;

/**
 * Based on Data structures and algorithms in java 6th edition, p.441
 * This class is primary for loading elements in a preorder fashion.
 * @param <E>
 */
public class WeightedLinkedTree<E> extends LinkedTree<E> {
    protected static class WeightedNode<E> extends Node<E> {

        private float weight = 0;

        public WeightedNode(E e, Node<E> parent, float weight) {
            super(e, parent);
            this.weight = weight;
        }

        public float getWeight() {
            return weight;
        }

        public void setWeight(float value) {
            weight = value;
        }

        public Node<E> getWeightedParent(float value) {
            if (weight > value) {
                return this;
            } else {
                return ((WeightedNode<E>)getParent()).getWeightedParent(value);
            }
        }
    }

    private Node<E> createNode(E e, Node<E> parent, float weight) {
        return new WeightedNode<>(e, parent, weight);
    }

    public Position<E> addChild(Position<E> p, E e, float weight) throws IllegalArgumentException {
        Position<E> weightedParent = weightedParent(p, weight);
        WeightedNode<E> parent = validate(weightedParent);
        Node<E> child = createNode(e, parent, weight);
        parent.addChild(child);
        size++;
        return child;
    }

    public float getWeight(Position<E> p) {
        WeightedNode<E> node = validate(p);
        return node.getWeight();
    }

    /**
     * Places E e at the root of an empty tree.
     * @param e
     * @return new Position
     * @throws IllegalStateException
     */
    @Override
    public Position<E> addRoot(E e) throws IllegalStateException {
        if (!isEmpty()) {
            throw new IllegalStateException("Tree is not empty");
        }
        root = createNode(e, null, Float.MAX_VALUE);
        size = 1;
        return root;
    }

    @Override
    protected WeightedNode<E> validate(Position<E> p) throws IllegalArgumentException {
        if (!(p instanceof WeightedNode)) {
            throw new IllegalArgumentException("Not valid position type");
        }
        WeightedNode<E> node = (WeightedNode<E>)p;
        if (node.getParent() == node) {
            throw new IllegalArgumentException("p is no longer in the tree");
        }
        return node;
    }

    /**
     * Gets the parent with a weight higher than given weight.
     * @param p
     * @param weight
     * @return
     * @throws IllegalStateException
     */
    public Position<E> weightedParent(Position<E> p, float weight) throws IllegalStateException {
        WeightedNode<E> node = validate(p);
        return node.getWeightedParent(weight);
    }
}
