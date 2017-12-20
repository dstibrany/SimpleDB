package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
    private ArrayList<Type> tupleTypes;
    private ArrayList<String> tupleFieldNames;

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
        ArrayList<Type> combinedTypes = new ArrayList<Type>();
        ArrayList<String> combinedFields = new ArrayList<String>();
        
        combinedTypes.addAll(td1.tupleTypes);
        combinedTypes.addAll(td2.tupleTypes);
        combinedFields.addAll(td1.tupleFieldNames);
        combinedFields.addAll(td2.tupleFieldNames);
        return new TupleDesc(
            combinedTypes.toArray(new Type[combinedTypes.size()]), 
            combinedFields.toArray(new String[combinedFields.size()])
        );
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        this.tupleTypes = new ArrayList<Type>(Arrays.asList(typeAr));
        this.tupleFieldNames = new ArrayList<String>(Arrays.asList(fieldAr));
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this.tupleTypes = new ArrayList<Type>(Arrays.asList(typeAr));
        this.tupleFieldNames = new ArrayList<String>(this.numFields());
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.tupleTypes.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        return this.tupleFieldNames.get(i);
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        int fieldIndex = this.tupleFieldNames.indexOf(name);
        if (fieldIndex == -1) throw new NoSuchElementException();
        return fieldIndex;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        return this.tupleTypes.get(i);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int tupleSize = 0;
        for (Type t: this.tupleTypes) {
            tupleSize += t.getLen();
        }
        return tupleSize;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == null || !(o instanceof TupleDesc)) {
            return false;
        }
        
        if (this.numFields() != ((TupleDesc)o).numFields()) {
            return false;
        }

        for (int i = 0; i < this.numFields(); i++) {
            if (this.getType(i) != ((TupleDesc)o).getType(i)) {
                return false;
            }
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        String output = "";
        for (int i = 0; i < this.numFields(); i++) {
            output += this.getType(i).name() + "(" +  this.getFieldName(i) + ")";
            if (i < this.numFields() - 1) {
                output += ", ";
            }
        }
        return output;
    }
}
