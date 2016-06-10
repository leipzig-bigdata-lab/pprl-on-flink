# Corrupt records for datasets from parties 2 to P
# The dataset of first party does not need to be corrupted
# Corruption is done using the GeCO module as below:
# 
# generate-data-english.py - Python module to generate and corrupt
#                           synthetic data based on
#                            English look-up and error tables.
#
# Peter Christen and Dinusha Vatsalan, January-March 2012
# =============================================================================
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# =============================================================================

# Import the necessary other modules of the data generator
#
import basefunctions  # Helper functions
import attrgenfunct   # Functions to generate independent attribute values
import contdepfunct   # Functions to generate dependent continuous attribute
                      # values
import generator      # Main classes to generate records and the data set
import corruptor      # Main classes to corrupt attribute values and records

import sys
import string
import os
import random
random.seed(42)  # Set seed for random generator, so data generation can be
                 # repeated

# Set the Unicode encoding for this data generation project. This needs to be
# changed to another encoding for different Unicode character sets.
# Valid encoding strings are listed here:
# http://docs.python.org/library/codecs.html#standard-encodings
#
unicode_encoding_used = 'ascii'

# The name of the record identifier attribute (unique value for each record).
# This name cannot be given as name to any other attribute that is generated.
#
rec_id_attr_name = 'recid'

attr_name_list = ['givenname', 'surname', 'suburb', 'postcode']
#attr_name_list = ['firstname', 'middlename', 'lastname', 'gender', 'DOA', 'address', 'city', 'zipcode', 'state']

in_file_name = sys.argv[1]
header  = True 
rec_id_col = sys.argv[2]
out_file_name = sys.argv[3] 

num_modification_per_record = sys.argv[4] #2
num_modification_per_record = int(num_modification_per_record)

ocp = int(sys.argv[5])

# Set the maximum number of duplicate records can be generated per original
# record.
#
max_duplicate_per_record = 1 

# Set the probability distribution used to create the duplicate records for one
# original record (possible values are: 'uniform', 'poisson', 'zipf').
#
num_duplicates_distribution = 'zipf'

# Set the maximum number of modification that can be applied to a single
# attribute (field).
#
max_modification_per_attr = 1

# Check if the given the unicode encoding selected is valid.
#
basefunctions.check_unicode_encoding_exists(unicode_encoding_used)

# -----------------------------------------------------------------------------

def char_set_nums(s):
  """Determine if the input string contains digits or not.
     Returns a string containing the set of corresponding characters.
  """
  char_set = '0123456789'

  return char_set

# -----------------------------------------------------------------------------
# Define how the generated records are to be corrupted (using methods from
# the corruptor.py module).

# For a value edit corruptor, the sum or the four probabilities given must
# be 1.0.
#
edit_corruptor = \
    corruptor.CorruptValueEdit(\
          position_function = corruptor.position_mod_normal,
          char_set_funct = basefunctions.char_set_ascii,
          insert_prob = 0.25,
          delete_prob = 0.25,
          substitute_prob = 0.25,
          transpose_prob = 0.25)

edit_pc_corruptor = \
    corruptor.CorruptValueEdit(\
          position_function = corruptor.position_mod_normal,
          char_set_funct = char_set_nums,
          insert_prob = 0.0,
          delete_prob = 0.0,
          substitute_prob = 1.0,
          transpose_prob = 0.0)

ocr_corruptor = corruptor.CorruptValueOCR(\
          position_function = corruptor.position_mod_normal,
          lookup_file_name = 'lookup-files/ocr-variations.csv',
          has_header_line = False,
          unicode_encoding = unicode_encoding_used)

keyboard_corruptor = corruptor.CorruptValueKeyboard(\
          position_function = corruptor.position_mod_normal,
          row_prob = 0.5,
          col_prob = 0.5)

phonetic_corruptor = corruptor.CorruptValuePhonetic(\
          lookup_file_name = 'lookup-files/phonetic-variations.csv',
          has_header_line = False,
          unicode_encoding = unicode_encoding_used)
# -----------------------------------------------------------------------------
# Define the attributes to be generated for this data set, and the data set
# itself.
#

## Read original database
#
rec_dict = {}  # Dictionary to contain the read records
non_mod_rec_dict = {}

if (in_file_name.lower().endswith('.gz')):
  in_file =  gzip.open(in_file_name)  # Open gzipped file
else:
  in_file =  open(in_file_name)       # Open normal file

# Skip header line if necessary
#
if (header == True):
  header_line = in_file.readline()  # Skip over header line

rec_count = 0

for rec in in_file:
  rec = rec.strip()
  rec = rec.split(',')
  clean_rec = map(string.strip, rec)  # Remove all surrounding whitespaces

  rec_id = 'rec-'
  if (rec_id_col == 'None'):
    rec_id += str(rec_count)  # Assign unique number as record identifier
  else:
    rec_id_col = int(rec_id_col)
    rec_id += clean_rec[rec_id_col]  # Get record identifier from file
  
  rec_id += '-org'

  assert rec_id not in rec_dict, ('Record ID not unique:', rec_id)

  if '_s' in rec_id: 
    rec_id = rec_id.replace('_s','')
    this_rec_attr_list = []
    for acol in [1,2,3,4]:
    #for acol in [1,2,3,4,5,6,7,8,9]:
      this_rec_attr_list.append(clean_rec[acol])
    rec_dict[rec_id] = this_rec_attr_list
  elif '_f' in rec_id: 
    rec_id = rec_id.replace('_f','')
    this_rec_attr_list = []
    for acol in [1,2,3,4]:
    #for acol in [1,2,3,4,5,6,7,8,9]:
      this_rec_attr_list.append(clean_rec[acol])
    rec_dict[rec_id] = this_rec_attr_list
  else:
    this_rec_attr_list = []
    for acol in [1,2,3,4]:
    #for acol in [1,2,3,4,5,6,7,8,9]:
      this_rec_attr_list.append(clean_rec[acol])
    non_mod_rec_dict[rec_id] = this_rec_attr_list

  rec_count += 1

num_org_rec = len(rec_dict)
print num_org_rec

ocp_recs = int(math.floor(num_org_rec * ocp/100.0))
num_dup_rec= ocp_recs #num_org_rec
print num_dup_rec

overlapping_recs = rec_dict.keys()
mod_recs = random.sample(overlapping_recs, num_dup_rec)

for rID in overlapping_recs:
  if rID not in mod_recs:
    non_mod_rec_dict[rID] = rec_dict[rID]
    del rec_dict[rID]

assert len(rec_dict) == num_dup_rec

print len(rec_dict), len(non_mod_rec_dict)

##################################################################

# Define the probability distribution of how likely an attribute will be
# selected for a modification.
# Each of the given probability values must be between 0 and 1, and the sum of
# them must be 1.0.
# If a probability is set to 0 for a certain attribute, then no modification
# will be applied on this attribute.
#
attr_mod_prob_dictionary = {'givenname':0.25, 'surname':0.25,'suburb':0.25,
                            'postcode':0.25}

#attr_mod_prob_dictionary = {'firstname':0.15, 'middlename':0.15,'lastname':0.15,
#                            'gender':0.0, 'DOA': 0.0, 'address': 0.10, 'city':0.15,
#                            'zipcode':0.15, 'state':0.15 }

# Define the actual corruption (modification) methods that will be applied on
# the different attributes.
# For each attribute, the sum of probabilities given must sum to 1.0.
#
attr_mod_data_dictionary = {'givenname':[(0.25, edit_corruptor),
                                       (0.25, ocr_corruptor),
                                       (0.25, keyboard_corruptor),
                                       (0.25, phonetic_corruptor)],
#                            'middlename':[(0.25, edit_corruptor),
#                                       (0.25, ocr_corruptor),
#                                       (0.25, keyboard_corruptor),
#                                       (0.25, phonetic_corruptor)],
                            'surname':[(0.25, edit_corruptor),
                                       (0.25, ocr_corruptor),
                                       (0.25, keyboard_corruptor),
                                       (0.25, phonetic_corruptor)],
#                            'gender':[(1.0, edit_corruptor),
#                                       (0.0, ocr_corruptor),
#                                       (0.0, keyboard_corruptor),
#                                       (0.0, phonetic_corruptor)],
#                            'DOA':[(1.0, edit_corruptor),
#                                       (0.0, ocr_corruptor),
#                                       (0.0, keyboard_corruptor),
#                                       (0.0, phonetic_corruptor)],
#                            'address':[(0.25, edit_corruptor),
#                                       (0.25, ocr_corruptor),
#                                       (0.25, keyboard_corruptor),
#                                       (0.25, phonetic_corruptor)],
                            'suburb':[(0.25, edit_corruptor),
                                       (0.25, ocr_corruptor),
                                       (0.25, keyboard_corruptor),
                                       (0.25, phonetic_corruptor)],
#                            'state':[(0.25, edit_corruptor),
#                                       (0.25, ocr_corruptor),
#                                       (0.25, keyboard_corruptor),
#                                       (0.25, phonetic_corruptor)],
                            'postcode':[(0.5, edit_pc_corruptor),
                                       (0.5, ocr_corruptor)]}


# Set-up the data set corruption object
#
test_data_corruptor = corruptor.CorruptDataSet(number_of_org_records = \
                                          num_dup_rec,
                                          number_of_mod_records = num_dup_rec,
                                          attribute_name_list = attr_name_list,
                                          max_num_dup_per_rec = \
                                                 max_duplicate_per_record,
                                          num_dup_dist = \
                                                 num_duplicates_distribution,
                                          max_num_mod_per_attr = \
                                                 max_modification_per_attr,
                                          num_mod_per_rec = \
                                                 num_modification_per_record,
                                          attr_mod_prob_dict = \
                                                 attr_mod_prob_dictionary,
                                          attr_mod_data_dict = \
                                                 attr_mod_data_dictionary)

# =============================================================================


# Corrupt (modify) the original records into duplicate records
#
rec_dict = test_data_corruptor.corrupt_records(rec_dict)

# Write generate data into a file
#
out_file = open(out_file_name,'w')
out_file.write(rec_id_attr_name)
for attr in attr_name_list:
  out_file.write(','+attr)
out_file.write(os.linesep)
for rec in rec_dict:
  if 'org' not in rec:
    rec_str_list = rec.split('-')
    rec_id = rec_str_list[1]
    out_file.write(rec_id)
    this_attr_val_list = rec_dict[rec]
    for ind in range(1,5):
    #for ind in range(1,10):
      if ind == 1:
        out_file.write(','+this_attr_val_list[0])
      elif ind == 2: 
        out_file.write(','+this_attr_val_list[1])
      elif ind == 3: 
        out_file.write(','+this_attr_val_list[2])
      elif ind == 4: 
        out_file.write(','+this_attr_val_list[3])
      #elif ind == 5: 
      #  out_file.write(','+this_attr_val_list[4])
      #elif ind == 6: 
      #  out_file.write(','+this_attr_val_list[5])
      #elif ind == 7: 
      #  out_file.write(','+this_attr_val_list[6])
      #elif ind == 8: 
      #  out_file.write(','+this_attr_val_list[7])
      #elif ind == 9: 
      #  out_file.write(','+this_attr_val_list[8])
      else:
        out_file.write(','+'NA')
    out_file.write(os.linesep)

for non_mod_rec in non_mod_rec_dict:
  out_file.write(non_mod_rec.split('-')[1])
  this_attr_val_list = non_mod_rec_dict[non_mod_rec]
  for ind in range(1,5):
  #for ind in range(1,10):
    if ind == 1:
      out_file.write(','+this_attr_val_list[0])
    elif ind == 2:
      out_file.write(','+this_attr_val_list[1])
    elif ind == 3:
      out_file.write(','+this_attr_val_list[2])
    elif ind == 4:
      out_file.write(','+this_attr_val_list[3])
    #elif ind == 5: 
    #  out_file.write(','+this_attr_val_list[4])
    #elif ind == 6: 
    #  out_file.write(','+this_attr_val_list[5])
    #elif ind == 7: 
    #  out_file.write(','+this_attr_val_list[6])
    #elif ind == 8: 
    #  out_file.write(','+this_attr_val_list[7])
    #elif ind == 9: 
    #  out_file.write(','+this_attr_val_list[8])
    else:
      out_file.write(','+'NA')
  out_file.write(os.linesep)

# End.
# =============================================================================
