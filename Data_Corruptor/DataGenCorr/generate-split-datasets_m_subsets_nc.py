# Python program to generate sets of data sets of different sizes extracted
# from one large single data set.
#
# Different sizes and overlaps can be selected
#
# It is assumed that the first column contains an entity identifier, and
# if the data set contains duplicate records of the same entiy then only the
# first will be used.

# DV,PC, November 2015

import gzip
import os
import random
import string

# The data set to use, it is assumed it contains attributes in the following
# columns (starting from column number 0): 1 = Given name,
# 2=Surname, 3=Suburb, 4=Postcode
# (this data set contains: 6,917,514 records)
#
in_file_name = 'ncvoter-temporal.csv'

overlap = 50  # overlap as percentage between data sets to be used

random.seed(30)

# ----------------------------------------------------------------------------
# Main program starts here

base_file_name = '.'.join(in_file_name.split('.')[:-1])  # Remove extension

# Step 1: Read input data into memory
#
if (in_file_name.lower().endswith('.gz')):
  in_file =  gzip.open(in_file_name)  # Open gzipped file
else:
  in_file =  open(in_file_name)  # Open normal file

header_line = in_file.readline()  # Read header line

in_data_list =    []
num_rec_in_data = 0
ent_id_set = set()

for line in in_file:
  line_list = line.strip().split(',')
  ent_id = line_list[0].strip()
  if ent_id not in ent_id_set:
    ent_id_set.add(ent_id)
    in_data_list.append(line_list)
    # print line_list
    num_rec_in_data += 1 
  #else:
    #print 'Duplicate record for entity:', ent_id

assert len(in_data_list) == num_rec_in_data

in_file.close()
print 'Read %d entity records' % (num_rec_in_data)
print

overlap_f = 0.01*overlap # Convert into ratio 0..1

# Main loop over is over the size tuple list - - - - - - - - - - - - - - - - - -
#
for sample_size in [5000, 10000, 50000, 100000, 500000, 1000000]: 

  print 'Sample size:', sample_size

  data_overlap_sample_size = sample_size/4 
  data_sub_overlap_sample_size = sample_size/4

  print '  Data overlap sample size:', data_overlap_sample_size
  print '  Data sub overlap sample size:', data_sub_overlap_sample_size

  # We sample three sets:
  # 1) overlap full set sample (the full set matching records)
  # 2) overlap sub set sample - (the sub set matching records)
  # 3) data sample - (non-matching records that occur in one of the databases only)

  in_num_list = range(num_rec_in_data)
  in_num_set = set(in_num_list)

  overlap_sample_set = set(random.sample(in_num_set, \
                           data_overlap_sample_size))

  overlap_recs = []
  sub_overlap_recs = []
  for x in overlap_sample_set:
    overlap_recs.append(in_data_list[x][0])

  # Remove overlap set from all input set
  #
  new_in_num_set = in_num_set - overlap_sample_set

  sub_overlap_sample_set = set(random.sample(new_in_num_set, \
                               data_sub_overlap_sample_size))

  #print sub_overlap_sample_set

  for x in sub_overlap_sample_set:
    sub_overlap_recs.append(in_data_list[x][0])

  snew_in_num_set = new_in_num_set - sub_overlap_sample_set

  parties = [1,2,3,4,5,6,7,8,9,10]

  subset_sizes = [2,3,4,5,6,7,8,9]

  alice_sample_set = []
  bob_sample_set = []
  charlie_sample_set = []
  p4_sample_set = []
  p5_sample_set = []
  p6_sample_set = []
  p7_sample_set = []
  p8_sample_set = []
  p9_sample_set = []
  p10_sample_set = []

  for s in sub_overlap_sample_set:
    subset_size = random.choice(subset_sizes)
    subset_parties = random.sample(parties,subset_size)
    for p in subset_parties:
      if p == 1:
        alice_sample_set.append(s) 
      elif p == 2:
        bob_sample_set.append(s)
      elif p == 3:
        charlie_sample_set.append(s)
      elif p == 4:
        p4_sample_set.append(s)
      elif p == 5:
        p5_sample_set.append(s)
      elif p == 6:
        p6_sample_set.append(s)
      elif p == 7:
        p7_sample_set.append(s)
      elif p == 8:
        p8_sample_set.append(s)
      elif p == 9:
        p9_sample_set.append(s)
      elif p == 10:
        p10_sample_set.append(s)

  alice_single_set_size = sample_size - (data_overlap_sample_size + len(alice_sample_set)) 
  alice_single_set = list(random.sample(snew_in_num_set,alice_single_set_size))
  alice_sample_set += alice_single_set
  snew_in_num_set -= set(alice_single_set)

  print 'Alice done'

  bob_single_set_size = sample_size - (data_overlap_sample_size + len(bob_sample_set))
  bob_single_set = list(random.sample(snew_in_num_set,bob_single_set_size))
  bob_sample_set += bob_single_set
  snew_in_num_set -= set(bob_single_set)

  print 'Bob done'

  charlie_single_set_size = sample_size - (data_overlap_sample_size + len(charlie_sample_set)) 
  charlie_single_set = list(random.sample(snew_in_num_set,charlie_single_set_size))
  charlie_sample_set += charlie_single_set
  snew_in_num_set -= set(charlie_single_set)
  print 'Charlie done' 

  p4_single_set_size = sample_size - (data_overlap_sample_size + len(p4_sample_set))
  p4_single_set = list(random.sample(snew_in_num_set,p4_single_set_size))
  p4_sample_set += p4_single_set
  snew_in_num_set -= set(p4_single_set)
  print 'p4 done'

  p5_single_set_size = sample_size - (data_overlap_sample_size + len(p5_sample_set)) 
  p5_single_set = list(random.sample(snew_in_num_set,p5_single_set_size))
  p5_sample_set += p5_single_set
  snew_in_num_set -= set(p5_single_set)
  print 'p5 done'

  p6_single_set_size = sample_size - (data_overlap_sample_size + len(p6_sample_set))
  p6_single_set = list(random.sample(snew_in_num_set,p6_single_set_size))
  p6_sample_set += p6_single_set
  snew_in_num_set -= set(p6_single_set)
  print 'p6 done'

  p7_single_set_size = sample_size - (data_overlap_sample_size + len(p7_sample_set))
  p7_single_set = list(random.sample(snew_in_num_set,p7_single_set_size))
  p7_sample_set += p7_single_set
  snew_in_num_set -= set(p7_single_set)
  print 'p7 done'

  p8_single_set_size = sample_size - (data_overlap_sample_size + len(p8_sample_set))
  p8_single_set = list(random.sample(snew_in_num_set,p8_single_set_size))
  p8_sample_set += p8_single_set
  snew_in_num_set -= set(p8_single_set)
  print 'p8 done'

  p9_single_set_size = sample_size - (data_overlap_sample_size + len(p9_sample_set))
  p9_single_set = list(random.sample(snew_in_num_set,p9_single_set_size))
  p9_sample_set += p9_single_set
  snew_in_num_set -= set(p9_single_set)
  print 'p9 done'

  p10_single_set_size = sample_size - (data_overlap_sample_size + len(p10_sample_set))
  p10_single_set = list(random.sample(snew_in_num_set,p10_single_set_size))
  p10_sample_set += p10_single_set
  snew_in_num_set -= set(p10_single_set)
  print 'p10 done'

  assert len(alice_sample_set) == sample_size - data_overlap_sample_size
  assert len(bob_sample_set) == sample_size - data_overlap_sample_size
  assert len(charlie_sample_set) == sample_size - data_overlap_sample_size
  assert len(p4_sample_set) == sample_size - data_overlap_sample_size
  assert len(p5_sample_set) == sample_size - data_overlap_sample_size
  assert len(p6_sample_set) == sample_size - data_overlap_sample_size
  assert len(p7_sample_set) == sample_size - data_overlap_sample_size
  assert len(p8_sample_set) == sample_size - data_overlap_sample_size
  assert len(p9_sample_set) == sample_size - data_overlap_sample_size
  assert len(p10_sample_set) == sample_size - data_overlap_sample_size

  assert overlap_sample_set.intersection(alice_sample_set) == set()
  assert overlap_sample_set.intersection(bob_sample_set) == set()
  assert overlap_sample_set.intersection(charlie_sample_set) == set()
  assert overlap_sample_set.intersection(p4_sample_set) == set()
  assert overlap_sample_set.intersection(p5_sample_set) == set()
  assert overlap_sample_set.intersection(p6_sample_set) == set()
  assert overlap_sample_set.intersection(p7_sample_set) == set()
  assert overlap_sample_set.intersection(p8_sample_set) == set()
  assert overlap_sample_set.intersection(p9_sample_set) == set()
  assert overlap_sample_set.intersection(p10_sample_set) == set()


  # Extract records from input data list and put into sample data sets
  #
  alice_data = []
  test_alice_data_set = set()
  for i in alice_sample_set:
    alice_data.append(in_data_list[i])
    test_alice_data_set.add(tuple(in_data_list[i]))

  bob_data = []
  test_bob_data_set = set()
  for i in bob_sample_set:
    bob_data.append(in_data_list[i])
    test_bob_data_set.add(tuple(in_data_list[i]))

  charlie_data = []
  test_charlie_data_set = set()
  for i in charlie_sample_set:
    charlie_data.append(in_data_list[i])
    test_charlie_data_set.add(tuple(in_data_list[i]))

  p4_data = []
  test_p4_data_set = set()
  for i in p4_sample_set:
    p4_data.append(in_data_list[i])
    test_p4_data_set.add(tuple(in_data_list[i]))

  p5_data = []
  test_p5_data_set = set()
  for i in p5_sample_set:
    p5_data.append(in_data_list[i])
    test_p5_data_set.add(tuple(in_data_list[i]))

  p6_data = []
  test_p6_data_set = set()
  for i in p6_sample_set:
    p6_data.append(in_data_list[i])
    test_p6_data_set.add(tuple(in_data_list[i]))

  p7_data = []
  test_p7_data_set = set()
  for i in p7_sample_set:
    p7_data.append(in_data_list[i])
    test_p7_data_set.add(tuple(in_data_list[i]))

  p8_data = []
  test_p8_data_set = set()
  for i in p8_sample_set:
    p8_data.append(in_data_list[i])
    test_p8_data_set.add(tuple(in_data_list[i]))

  p9_data = []
  test_p9_data_set = set()
  for i in p9_sample_set:
    p9_data.append(in_data_list[i])
    test_p9_data_set.add(tuple(in_data_list[i]))

  p10_data = []
  test_p10_data_set = set()
  for i in p10_sample_set:
    p10_data.append(in_data_list[i])
    test_p10_data_set.add(tuple(in_data_list[i]))

  assert len(alice_data) == sample_size - data_overlap_sample_size
  assert len(bob_data) ==   sample_size - data_overlap_sample_size
  assert len(charlie_data) ==  sample_size - data_overlap_sample_size
  assert len(p4_data) == sample_size - data_overlap_sample_size
  assert len(p5_data) == sample_size - data_overlap_sample_size
  assert len(p6_data) == sample_size - data_overlap_sample_size
  assert len(p7_data) == sample_size - data_overlap_sample_size
  assert len(p8_data) == sample_size - data_overlap_sample_size
  assert len(p9_data) == sample_size - data_overlap_sample_size
  assert len(p10_data) == sample_size - data_overlap_sample_size

  assert len(test_alice_data_set) == sample_size - data_overlap_sample_size
  assert len(test_bob_data_set) ==   sample_size - data_overlap_sample_size
  assert len(test_charlie_data_set) == sample_size - data_overlap_sample_size
  assert len(test_p4_data_set) ==   sample_size - data_overlap_sample_size
  assert len(test_p5_data_set) ==   sample_size - data_overlap_sample_size
  assert len(test_p6_data_set) == sample_size - data_overlap_sample_size
  assert len(test_p7_data_set) == sample_size - data_overlap_sample_size
  assert len(test_p8_data_set) == sample_size - data_overlap_sample_size
  assert len(test_p9_data_set) == sample_size - data_overlap_sample_size
  assert len(test_p10_data_set) == sample_size - data_overlap_sample_size

  print 'Non-overlap sampled data containing %d, %d, %d, %d, %d, %d, %d, %d, %d, and %d records' % \
         (len(alice_data), len(bob_data), len(charlie_data), len(p4_data), len(p5_data), \
         len(p6_data), len(p7_data), len(p8_data), len(p9_data),len(p10_data) )

  # Append overlap records to both data sets
  #
  test_overlap_data_set = set()
  for i in overlap_sample_set:
    alice_data.append(in_data_list[i])
    bob_data.append(in_data_list[i])
    charlie_data.append(in_data_list[i])
    p4_data.append(in_data_list[i])
    p5_data.append(in_data_list[i])
    p6_data.append(in_data_list[i])
    p7_data.append(in_data_list[i])
    p8_data.append(in_data_list[i])
    p9_data.append(in_data_list[i])
    p10_data.append(in_data_list[i])
    test_overlap_data_set.add(tuple(in_data_list[i]))

  assert len(test_overlap_data_set) == data_overlap_sample_size

  print '  Alice number of records:', len(alice_data)
  print '  Bob number of records:  ', len(bob_data)
  print '  Charlie number of records:  ', len(charlie_data)
  print '  P4 number of records:  ', len(p4_data)
  print '  P5 number of records:  ', len(p5_data)
  print '  P6 number of records:  ', len(p6_data)
  print '  P7 number of records:  ', len(p7_data)
  print '  P8 number of records:  ', len(p8_data)
  print '  P9 number of records:  ', len(p9_data)
  print '  P10 number of records:  ', len(p10_data)
  print

  # Write sample data sets without modifciations - - - - - - - - - - - - - - -
  #

  file_name_a = 'ncvr_numrec_%d_modrec_2_ocp_0_myp_0_nump_10.csv' % \
                (sample_size)
  file_name_b = 'ncvr_numrec_%d_modrec_2_ocp_0_myp_1_nump_10.csv' % \
                (sample_size)
  file_name_c = 'ncvr_numrec_%d_modrec_2_ocp_0_myp_2_nump_10.csv' % \
                (sample_size)
  file_name_d = 'ncvr_numrec_%d_modrec_2_ocp_0_myp_3_nump_10.csv' % \
                (sample_size)
  file_name_e = 'ncvr_numrec_%d_modrec_2_ocp_0_myp_4_nump_10.csv' % \
                (sample_size)
  file_name_f = 'ncvr_numrec_%d_modrec_2_ocp_0_myp_5_nump_10.csv' % \
                (sample_size)
  file_name_g = 'ncvr_numrec_%d_modrec_2_ocp_0_myp_6_nump_10.csv' % \
                (sample_size)
  file_name_h = 'ncvr_numrec_%d_modrec_2_ocp_0_myp_7_nump_10.csv' % \
                (sample_size)
  file_name_i = 'ncvr_numrec_%d_modrec_2_ocp_0_myp_8_nump_10.csv' % \
                (sample_size)
  file_name_j = 'ncvr_numrec_%d_modrec_2_ocp_0_myp_9_nump_10.csv' % \
                (sample_size)

  # First datasets - no corruptions need to be done
  out_file_a = open(file_name_a,'w')
  out_file_a.write(header_line)
  for rec in alice_data:
    line = rec[0]+','+rec[3]+','+ rec[5]+','+ rec[12]+','+ rec[14]+os.linesep
    #line = rec[0]+','+rec[3]+','+rec[4]+','+rec[5] +','+rec[8] +','+rec[17] +','+rec[11] +','+ rec[12]+','+ rec[14] +','+rec[13] +os.linesep
    out_file_a.write(line)
  out_file_a.close()
  

  out_file_b = open(file_name_b,'w')
  out_file_b.write(header_line)
  for rec in bob_data:
    if rec[0] in overlap_recs:
      rID = rec[0]+'_f'
    else if rec[0] in sub_overlap_recs:
      rID = rec[0]+'_s'
    else:
      rID = rec[0]
    line = rID+','+rec[3]+','+ rec[5]+','+ rec[12]+','+ rec[14]+os.linesep
    out_file_b.write(line)
  out_file_b.close()

  out_file_c = open(file_name_c,'w')
  out_file_c.write(header_line)
  for rec in charlie_data:
    if rec[0] in overlap_recs:
      rID = rec[0]+'_f'
    else if rec[0] in sub_overlap_recs:
      rID = rec[0]+'_s'
    else:
      rID = rec[0]
    line = rID+','+rec[3]+','+ rec[5]+','+ rec[12]+','+ rec[14]+os.linesep
    out_file_c.write(line)
  out_file_c.close()

  out_file_d = open(file_name_d,'w')
  out_file_d.write(header_line)
  for rec in p4_data:
    if rec[0] in overlap_recs:
      rID = rec[0]+'_f'
    else if rec[0] in sub_overlap_recs:
      rID = rec[0]+'_s'
    else:
      rID = rec[0]
    line = rID+','+rec[3]+','+ rec[5]+','+ rec[12]+','+ rec[14]+os.linesep
    out_file_d.write(line)
  out_file_d.close()

  out_file_e = open(file_name_e,'w')
  out_file_e.write(header_line)
  for rec in p5_data:
    if rec[0] in overlap_recs:
      rID = rec[0]+'_f'
    else if rec[0] in sub_overlap_recs:
      rID = rec[0]+'_s'
    else:
      rID = rec[0]
    line = rID+','+rec[3]+','+ rec[5]+','+ rec[12]+','+ rec[14]+os.linesep
    out_file_e.write(line)
  out_file_e.close()

  out_file_f = open(file_name_f,'w')
  out_file_f.write(header_line)
  for rec in p6_data:
    if rec[0] in overlap_recs:
      rID = rec[0]+'_f'
    else if rec[0] in sub_overlap_recs:
      rID = rec[0]+'_s'
    else:
      rID = rec[0]
    line = rID+','+rec[3]+','+ rec[5]+','+ rec[12]+','+ rec[14]+os.linesep
    out_file_f.write(line)
  out_file_f.close()

  out_file_g = open(file_name_g,'w')
  out_file_g.write(header_line)
  for rec in p7_data:
    if rec[0] in overlap_recs:
      rID = rec[0]+'_f'
    else if rec[0] in sub_overlap_recs:
      rID = rec[0]+'_s'
    else:
      rID = rec[0]
    line = rID+','+rec[3]+','+ rec[5]+','+ rec[12]+','+ rec[14]+os.linesep
    out_file_g.write(line)
  out_file_g.close()

  out_file_h = open(file_name_h,'w')
  out_file_h.write(header_line)
  for rec in p8_data:
    if rec[0] in overlap_recs:
      rID = rec[0]+'_f'
    else if rec[0] in sub_overlap_recs:
      rID = rec[0]+'_s'
    else:
      rID = rec[0]
    line = rID+','+rec[3]+','+ rec[5]+','+ rec[12]+','+ rec[14]+os.linesep
    out_file_h.write(line)
  out_file_h.close()

  out_file_i = open(file_name_i,'w')
  out_file_i.write(header_line)
  for rec in p9_data:
    if rec[0] in overlap_recs:
      rID = rec[0]+'_f'
    else if rec[0] in sub_overlap_recs:
      rID = rec[0]+'_s'
    else:
      rID = rec[0]
    line = rID+','+rec[3]+','+ rec[5]+','+ rec[12]+','+ rec[14]+os.linesep
    out_file_i.write(line)
  out_file_i.close()

  out_file_j = open(file_name_j,'w')
  out_file_j.write(header_line)
  for rec in p10_data:
    if rec[0] in overlap_recs:
      rID = rec[0]+'_f'
    else if rec[0] in sub_overlap_recs:
      rID = rec[0]+'_s'
    else:
      rID = rec[0] 
    line = rID+','+rec[3]+','+ rec[5]+','+ rec[12]+','+ rec[14]+os.linesep
    out_file_j.write(line)
  out_file_j.close()

print 'End.'
