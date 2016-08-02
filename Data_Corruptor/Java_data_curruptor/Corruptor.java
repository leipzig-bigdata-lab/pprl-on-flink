public class Corruptor {

    public static int position_mod_uniform() {
        // select any position in the given input string with uniform likelihood
        // Return 0 if the string is empty
        // TODO
    }

    public static int position_mod_normal() {
        // select any position in the given input string with normally distributed
        // likelihood where the average of the normal distribution is set to one
        // character behind the middle of the string, and the standard deviation is
        // set to 1/4 of the string length
        //
        // This is based on studies on the distribution of errors in real text which
        // showed that errors such as typographical mistakes are more likely to
        // appear towards the middle and end of a string but not at the beginning.
        //
        // Return 0 if the string is empty.
        //
        // TODO
    }

    private class CorruptValue {
        // Base class for the definition of corruptor that is applied on a single
        // attribute (field) in the data set.
        //
        // This class and all of its derived classes provide methods that allow
        // the definition of how values in a single attribute are corrupted (modified)
        // and the parameters necessary for the corruption process.
        //
        // The following variables need to be set when a CorruptValue instance is
        // initialised (with further parameters listed in the derived process):
        //
        // position_function    A function that (somehow) determines the location
        //                      within a string value of where a modification
        //                      (corruption) is to be applied. The input of this
        //                      function is assumed to be a strign and its return 
        //                      value an integer number in the range of the length
        //                      of the given input string.o
        //

        CorruptValue() {
            // constructor, set general attributes
            //TODO
        }
        
        public static void corrupt_value() {
            // method which corrupts the given input string and returns the modified
            // string
            // See implementation in derived classes for details
            // TODO
        }

    }

    private class CorruptMissingValue() {
        // A corruptor method which simply sets an attribute value to a missing value
        //
        // the additional argument (besides the base class argument
        // 'position_function') that has to be set when this attribute type is
        // initialised are:
        //
        // missing_val  The string which sedignates a missing value. Default value
        //              is the empty string ''.
        //
        // Note that the 'position_function' is not required by this corruptor method
        

        CorruptMissingValue() {
            // Constructor. Process the derived keywords first, then call the base
            // class constructor
        }

        public static String corrupt_value() {
            // simply return the missing value string 
            // TODO
        }
    }

    private class CorruptValueEdit() {
        // A simple corruptor which applies one edit operation on the given value.
        //
        // Depending upon the content of the value (letters, digits, or mixed),
        // if the edit operation is an insert of substitution a character from the
        // same set (letters, digits or both) is selected.
        //
        // The additional arguments (besides the base class argument
        // 'position_function') that has to be set when this attribute type is
        // initialised are:
        //
        // char_set_func    A function which determines the set of characters that
        //                  can be inserted or used of substitution
        // insert_prob      These for values set the likelihoodof which edit
        // delete_prob      operation will be selected.
        // substitute_prob  All four probability values must be between 0 and 1, and
        // transpose_prob   the sum of these four values must be 1.0
        //
        CorruptValueEdit() {
            // Constructor. Process the derived keywords first, then call the base
            // class constructor
        }

        public static void corrupt_value() {
            // method which corrupts the given input string and returns the modified
            // string by randomly selecting an edit operation and position in the
            // string where to apply this edit.
        }
    }

    private class CorruptValueKeyboard() {
        // Use a keyboard layout to simulate typing errors. they keyboard is hard-
        // coded into this method, but can be changed easily for different
        // keyboard layout.
        //
        // A character form the original input string will be randomly chose using
        // the position function, and the a character form either the same row or
        // column in the keyboard will be selected.
        //
        // The additional arguments (beside the base class argument
        // 'position_function') that have to be set when this attribute type is
        // initialised are:
        //
        // row_prob     The probability that a neighbouring character in the same
        //              row is selected.
        // col_prob     The probability that a neighbouring character in the same
        //              column is selected
        //
        // The sum of row_prob and col_prob must be 1.0
        //
        
        CorruptValueKeyboard() {
            // Construcotr. Process the derived keywords first, then call the base
            // class constructor
            // TODO
        }

        public static String corrupt_value() {
            // method which corrupts the given input string by replacing a single
            // character with a neighboring character given the defined
            // keyboard layout at a position randomly selected by the position
            // function.
            // TODO
        }
    }


}
