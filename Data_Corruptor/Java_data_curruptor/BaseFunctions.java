

public class BaseFunctions() {

    public static boolean check_is_not_none(Class<Object> variable) {

        // Check if the value given is not Null

        if (variable.isNull) {
            return true;
        }
                
        return false;
    } 

    public static boolean check_is_string(String string) { 
        // Check if the value given is of type string.
        
        if (string instanceof String){
            return true;
        }

        return false;
            
    }

    public static boolean check_is_unicode_string(String string){

        // Check if the value given is of ty[e unicode string
        // TODO
    }

    public static boolean check_is_string_or_unicode_string() {
        // Check if the value given is of type string of unicode string

        //TODO
    }

    public static boolean check_is_non_empty_string() {
        // Check if the value given is of type string and is not
        // an empty string
        
        //TODO
    }

    public static boolean check_is_number() {
        // Check if the value given is a number, i.e. of type integer or float
        // TODO
    }

    public static boolean check_is_positive() {
        // Check if the value given is a positive number, i.e. of type
        // integer or float, and greater than zero.
        // TODO
    }
    
    public static boolean check_is_not_negative() {
        // Check if the value given is a non-negative number, i.e. of type
        // integer of float, and greater than or equal to zero
        // TODO
        // HAVE to check if that method is really necessary...
    }

    public static boolean check_is_normalised() {
        // Check if the value given is a number, i.e. of type integer or float,
        // and between (including) 0.0 and 1.0
        // TODO
    }

    public static boolean check_is_percentage() {
        // Check if the value given is a number, i.e. of type integer of float, and
        // between (including) 0 and 100
        // TODO
    }

    public static boolean check_is_integer() {
        // Check if the value given is an integer number
        // TODO
    }

    public static boolean check_if_float() {
        // Check if the value given is a floating-point number
        // TODO
    }

    public static boolean check_is_dictionary() {
        // Check if the value given is of type dictionary
        // TODO
        // there's no dictionary in java !!!
    }

    public static boolean check_is_list() {
        // check if the value given is of type list
        // TODO
        // check difference between java list and python list
    }

    public static boolean check_is_set() {
        // Check if the value given is of type set
        // TODO
        // check difference between java set and python set
    }

    public static boolean check_is_tuple() {
        // check if the value given is of type tuple
        // TODO
        // check difference between java tuple and python tuple
    }

    public static boolean check_is_flag() {
        // Check if the value given is either True or False
        // TODO
    }
    
    public static boolean check_is_function_or_method() {
        // Check if the value given is a function or method
        // TODO
    }

    public static boolean check_unicode_encoding_exists() {
        // Checks if the given Unicode encoding string is know to the Python
        // codec registry
        //
        // TODO
        // have to check this!!!
    }

    public static String char_set_ascii() {
        // Determine if the input string contains digits, letters, or both, as well
        // as whitespaces or not
        // Returns a string containing the set of corresponding characters
        // TODO
    }

    public static boolean check_is_valid_format_str() {
        // Check if the value given is a valid formatting string for numbers
        // possible formatting values are:
        // int, float1, float2, float3, float4, float5, float6, float7, float8, or
        // float9
        // TODO
    }

    public static String float_to_str() {
        // Convert the given floating-point (or integer) number into a string
        // according to the format string given
        // The format stirng can be one of 'int' (return a string that corresponds to
        // an integer value), or 'float1', 'float2', ..., 'float9' which returns a
        // string of the number with the specified number of digits behind the comma.
        // TODO
    }

    public static List[] str2comma_separated_lists() {
        // Splits the values in a list at commas, and checks all values
        // if they are quoted (double or single) at both ends or not. Quotes
        // are removed.
        // Note that this function will split values that are quoted but contain one
        // or more commas into several values.
        // TODO
    }

    public static void read_csv_file() {
        // Read a comma seperated values (CSV) file from disk using the given Unicode
        // encoding
        //
        // Arguments:
        // file_name    Name of the file to read
        // encoding     The name of a Unicode encoding to be used when reading the file
        //              If set to None then the standard 'ascii' encoding will be used
        //
        // header_line  A flag, set to True of False, that has to be set according
        //              to if the frequency file starts with a header line or not.
        //
        // This function returns two items:
        // - If given, a list that contains the values in the header line of the file.
        //   If no header line was given, this item  will be set to None.
        //
        // - A list containing the records in the CSV file, each as a list
        //
        // Notes:
        // - Lines starting with # are assumed to contain comments and will be skipped. Lines that are empty will also be skipped.
        // - The CSV files must not contain commas in the values, while values
        // in quotes (double or single) can be handled
        //
        // TODO
    }

    public static void write_csv_file() {
        // Write a comma seperated values (CSV) file to disk using the given Unicode encoding.
        // Arguments:
        // file_name    Name of the file to write
        // encoding     The name of a Unicode encoding to be used when reading the file
        //              If set to None then the standard 'ascii' encoding will be used
        // header_list  A list containing the attribute (field) names to be written at the beginning of the file.
        //              If no header line is to be written then this argument needs to be set to None.
        // file_data    A list containing the records to be written into the CSV file.
        //              Each record must be a list of values, and these values will
        //              be concatenated with commas and written into the file.
        //              It is assumed the values given do not contain comas.
        //
        // TODO
    }

}
